import logging
import requests
from transformers import MarianMTModel, MarianTokenizer
import nltk
from nltk.tokenize import sent_tokenize
import time
import os
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from time import monotonic, sleep
from typing import Optional, Dict

# Ensure NLTK punkt tokenizer is available
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    try:
        nltk.download('punkt', quiet=True)
    except Exception:
        pass

# Quiet noisy libs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---- Global translate client session + concurrency guard --------------
_TRANSLATE_SESSION: Optional[requests.Session] = None
_TRANSLATE_SESSION_LOCK = threading.Lock()
TRANSLATE_CONCURRENCY = int(os.environ.get("TRANSLATE_CONCURRENCY", "3"))
_TRANSLATE_SEM = threading.BoundedSemaphore(TRANSLATE_CONCURRENCY)


def _respect_retry_after(resp: requests.Response) -> float:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return 0.0
    try:
        return max(float(ra), 0.0)
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime
            from datetime import datetime, timezone

            dt = parsedate_to_datetime(ra)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max((dt - datetime.now(timezone.utc)).total_seconds(), 0.0)
        except Exception:
            return 0.0


def _get_translate_session() -> requests.Session:
    global _TRANSLATE_SESSION
    if _TRANSLATE_SESSION is None:
        with _TRANSLATE_SESSION_LOCK:
            if _TRANSLATE_SESSION is None:
                s = requests.Session()
                adapter = HTTPAdapter(
                    pool_connections=64,
                    pool_maxsize=256,
                    max_retries=Retry(
                        total=2,
                        backoff_factor=0.3,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=False,
                    ),
                )
                s.mount("http://", adapter)
                s.mount("https://", adapter)
                _TRANSLATE_SESSION = s
    return _TRANSLATE_SESSION


# ---- Per-country Session Pool -----------------------------------------
_SESSION_POOL: Dict[str, requests.Session] = {}
_SESSION_POOL_LOCK = threading.Lock()


def get_country_session(country: Optional[str]) -> requests.Session:
    key = (country or "default").lower()
    sess = _SESSION_POOL.get(key)
    if sess is None:
        with _SESSION_POOL_LOCK:
            sess = _SESSION_POOL.get(key)
            if sess is None:
                sess = requests.Session()
                adapter = HTTPAdapter(
                    pool_connections=20,
                    pool_maxsize=100,
                    max_retries=Retry(
                        total=2,
                        backoff_factor=0.3,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=False,
                    ),
                )
                sess.mount("http://", adapter)
                sess.mount("https://", adapter)
                _SESSION_POOL[key] = sess
    return sess


def close_all_sessions() -> None:
    # Close country-specific sessions
    for s in list(_SESSION_POOL.values()):
        try:
            s.close()
        except Exception:
            pass
    _SESSION_POOL.clear()

    # Close global translation session
    global _TRANSLATE_SESSION
    if _TRANSLATE_SESSION is not None:
        try:
            _TRANSLATE_SESSION.close()
        except Exception:
            pass
        _TRANSLATE_SESSION = None


# -----------------------------------------------------------------------
COUNTRY_DELAY_SEC = {
    "denmark": 10,
    "estonia": 60,
    # defaults
    "malta": 1,
    "poland": 1,
    "finland": 1,
    "croatia": 1,
    "bulgaria": 1,
    "belgium": 1,
    "netherlands": 1,
    "lithuania": 1,
    "czech": 1,
    "romania": 1,
    "luxembourg": 1,
    "germany": 1,
    "austria": 1,
    "greece": 1,
    "italy": 1,
    "france": 1,
    "portugal": 1,
    "spain": 1,
    "ireland": 1,
    "hungary": 1,
    "slovakia": 1,
    "sweden": 1,
    "cyprus": 1,
    "latvia": 1,
    "slovenia": 1,
}

from collections import defaultdict


class CountryRateLimiter:
    def __init__(self, delays_sec: Dict[str, float], default_sec: float = 1.0):
        self.delays = {k.lower(): float(v) for k, v in delays_sec.items()}
        self.default = float(default_sec)
        self._last_call: Dict[str, float] = {}
        self._locks = defaultdict(threading.Lock)  # per-country lock

    def wait(self, country: str) -> None:
        key = (country or "").lower()
        delay = self.delays.get(key, self.default)
        if delay <= 0:
            return

        lock = self._locks[key]
        with lock:
            now = monotonic()
            last = self._last_call.get(key, 0.0)
            next_allowed = max(last + delay, now)
            wait_for = next_allowed - now
            if wait_for > 0:
                sleep(wait_for)
            self._last_call[key] = monotonic()


rate_limiter = CountryRateLimiter(COUNTRY_DELAY_SEC)


def throttled_get(
    url: str,
    country: Optional[str],
    *,
    timeout: float = 10,
    session: Optional[requests.Session] = None,
    **kwargs,
) -> requests.Response:

    rate_limiter.wait(country or "")
    sess = session or get_country_session(country)
    resp = sess.get(url, timeout=timeout, **kwargs)
    if resp.status_code in (429, 503):
        delay = _respect_retry_after(resp)
        if delay > 0:
            time.sleep(delay + 0.1)
            rate_limiter.wait(country or "")
            resp = sess.get(url, timeout=timeout, **kwargs)
    return resp


# ----------------------------------------------------------------------
# Translation pipeline


def translate(q: str, source: str, target: str) -> str:
    """
    Enhanced translation with: local service -> Google endpoint -> googletrans fallback.
    Respects concurrency & Retry-After.
    """
    if not q or not str(q).strip():
        return q

    # 1) Try local LibreTranslate-compatible service
    local_url = os.environ.get("LOCAL_TRANSLATE_URL", "http://127.0.0.1:5000/translate")
    data = {"q": q, "source": source, "target": target, "format": "text"}
    try:
        with _TRANSLATE_SEM:
            sess = _get_translate_session()
            response = sess.post(local_url, json=data, timeout=10)
        if response.status_code in (429, 503):
            delay = _respect_retry_after(response)
            if delay > 0:
                time.sleep(delay + 0.1)
                with _TRANSLATE_SEM:
                    response = sess.post(local_url, json=data, timeout=10)
        response.raise_for_status()
        js = response.json()
        if isinstance(js, dict) and "translatedText" in js:
            return js["translatedText"]
        # Some LT-compatible servers return a list of segments
        if isinstance(js, list):
            return " ".join(str(x) for x in js if x)
        raise ValueError("Unexpected response shape from local translator")
    except Exception as e:
        logger.warning(f"Local translation service failed ({e}). Fallback to Google endpoint...")

    # 2) Fallback to Google Translate (unofficial)
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {"client": "gtx", "sl": source, "tl": target, "dt": "t", "q": q}
        with _TRANSLATE_SEM:
            sess = _get_translate_session()
            response = sess.get(url, params=params, timeout=10)
        if response.status_code in (429, 503):
            delay = _respect_retry_after(response)
            if delay > 0:
                time.sleep(delay + 0.1)
                with _TRANSLATE_SEM:
                    response = sess.get(url, params=params, timeout=10)
        response.raise_for_status()
        result = response.json()
        if result and len(result) > 0 and len(result[0]) > 0:
            translated_text = "".join([part[0] for part in result[0] if part and part[0]])
            return translated_text
        raise ValueError("Empty response from Google Translate")
    except Exception as e:
        logger.error(f"Direct Google Translate API failed: {e}. Trying googletrans fallback...")

        # 3) Final fallback to googletrans (lazy import to avoid hard dep)
        try:
            from googletrans import Translator  # lazy import

            translator = Translator()
            result = translator.translate(q, src=source, dest=target)
            # Handle async coroutine if needed
            if hasattr(result, "__await__"):
                import asyncio

                result = asyncio.run(result)
            return result.text
        except Exception as e2:
            logger.error(f"Googletrans fallback also failed: {e2}")
            return q  # Return original text if all translation methods fail


def translateMT(text: str, source: str) -> str:
    """
    MarianMT translation with fallback to Google Translate -> googletrans.
    source -> en
    """
    # Import torch only when needed (keeps import time low)
    try:
        import torch  # noqa: F401
    except Exception as e:
        logger.warning(f"PyTorch not available ({e}); using fallback translators...")
        # go straight to fallback
        return translate(text, source=source, target="en")

    try:
        model_name = f"Helsinki-NLP/opus-mt-{source}-en"
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)

        sentences = sent_tokenize(text)
        translated_sentences = []

        for sentence in sentences:
            tokens = tokenizer(sentence, return_tensors="pt", padding=True, truncation=False)

            # If the sentence exceeds 512 tokens, split into chunks
            input_ids = tokens["input_ids"][0]
            if input_ids.size(0) > 512:
                # split into 512-token chunks
                chunks = [input_ids[i : i + 512] for i in range(0, input_ids.size(0), 512)]
                for chunk in chunks:
                    # build minimal tensors
                    attn = (chunk != tokenizer.pad_token_id).to(dtype=torch.long)
                    chunk_tensor = {
                        "input_ids": chunk.unsqueeze(0),
                        "attention_mask": attn.unsqueeze(0),
                    }
                    translated = model.generate(**chunk_tensor)
                    tgt_text = tokenizer.decode(translated[0], skip_special_tokens=True)
                    translated_sentences.append(tgt_text)
            else:
                translated = model.generate(**tokens)
                tgt_text = tokenizer.decode(translated[0], skip_special_tokens=True)
                translated_sentences.append(tgt_text)

        return " ".join(translated_sentences)

    except Exception as e:
        logger.warning(f"MarianMT translation failed for {source}: {e}. Using fallback...")

        # Fallback to Google Translate (unofficial)
        try:
            url = "https://translate.googleapis.com/translate_a/single"
            params = {"client": "gtx", "sl": source, "tl": "en", "dt": "t", "q": text}
            with _TRANSLATE_SEM:
                sess = _get_translate_session()
                response = sess.get(url, params=params, timeout=10)

            if response.status_code in (429, 503):
                delay = _respect_retry_after(response)
                if delay > 0:
                    time.sleep(delay + 0.1)
                    with _TRANSLATE_SEM:
                        response = sess.get(url, params=params, timeout=10)

            response.raise_for_status()
            result = response.json()
            if result and len(result) > 0 and len(result[0]) > 0:
                translated_text = "".join([part[0] for part in result[0] if part and part[0]])
                logger.info(f" Google Translate fallback successful for {source}")
                return translated_text
            raise ValueError("Empty response from Google Translate")

        except Exception as e2:
            logger.error(f"Google Translate fallback also failed for {source}: {e2}")

            # Final fallback to googletrans
            try:
                from googletrans import Translator  # lazy import

                translator = Translator()
                result = translator.translate(text, src=source, dest="en")
                if hasattr(result, "__await__"):
                    import asyncio

                    result = asyncio.run(result)
                logger.info(f" googletrans fallback successful for {source}")
                return result.text
            except Exception as e3:
                logger.error(f"All translation methods failed for {source}: {e3}")
                return text  # Return original text if all methods fail


def check_translation_service() -> bool:
    """
    Check if the local translation service is available
    """
    url = os.environ.get("LOCAL_TRANSLATE_URL", "http://127.0.0.1:5000/translate")
    try:
        test_data = {"q": "hello", "source": "en", "target": "el", "format": "text"}
        with _TRANSLATE_SEM:
            sess = _get_translate_session()
            r = sess.post(url, json=test_data, timeout=5)
        if r.status_code == 200:
            print(" Translation service is running")
            return True
        else:
            print(" Translation service responded with", r.status_code, r.text[:200])
            return False
    except Exception as e:
        print(" Translation service is not running:", str(e))
        print("\n📋 To start the translation service, run one of these commands:")
        print("1. Install and start LibreTranslate:")
        print("   pip install libretranslate")
        print("   libretranslate --host 127.0.0.1 --port 5000")
        print("\n2. Or use Docker:")
        print("   docker run -it --rm -p 5000:5000 libretranslate/libretranslate")
        print("\n3. Or use the web interface:")
        print("   cd web_interface")
        print("   python app.py")
        print("\n The script will continue using fallback translation methods.")
        return False


# Check translation service on import
if __name__ == "__main__":
    check_translation_service()

