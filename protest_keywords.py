import re

# Define protest keywords and prefix mapping
PREFIX_MAP = {
    "protest"    : "protest",
    "gather"     : "gather",
    "mobiliz"    : "mobilize",
    "strik"      : "strike",
    "struck"     : "strike",
    "demonstrat" : "demonstrate",
    "demo"       : "demonstration",
    "activis"    : "activism",
    "boycott"    : "boycott",
    "walkout"    : "walkout",
    "march"      : "march",
    "stoppag"    : "stoppage",
    "workstop"   : "workstop",
    "picket"     : "picket",
    "occupat"   : "occupation",
    "rall"       : "rally",
    "riot"       : "riot",
    "blocad"    : "blocade",
    "blocκ"    : "blocade",
    "blocade"    : "blocade",
}
# EU countries list
EU_COUNTRIES = {
    'austria', 'belgium', 'bulgaria', 'croatia', 'cyprus', 'czech republic',
    'denmark', 'estonia', 'finland', 'france', 'germany', 'greece', 'hungary', 'ireland',
    'italy', 'latvia', 'lithuania', 'luxembourg', 'malta', 'netherlands', 'poland',
    'portugal', 'romania', 'slovakia', 'slovenia', 'spain', 'sweden', 'europe'
}


root_words = ['protest', 'protested', 'protesters', 'protesting', 'demonstration', 'demonstrations', 'demonstrator',
              'rally', 'rallies', 'boycott', 'activism', 'activist', 'strike', 'strik', 'strikes', 'walkout',
              'stoppage', 'workstop', 'gather', 'gathered', 'stroke', 'occupy', 'occupied', 'occupies', 'mobilized',
              'mobilize']

CSV_FIELDS = [
    'label', 'url', 'keys', 'sorted_keys', 'text', 'title',
    'translated_title', 'content', 'translated_content', 'summary',
    'translated_summary', 'keywords', 'translated_keywords',
    'processed', 'name', 'language',
    'publication_date', 'lastmod', 'author', 'status'
]


GREEK_PROTEST_STEMS = [
    "ΔΙΑΜΑΡΤ", "ΔΙΑΔΗΛΩ", "ΣΥΓΚΕΝΤΡΩ",
    "ΑΚΤΙΒΙΣ", "ΑΠΕΡΓ", "ΔΙΑΚΟΠ",
    "ΔΙΕΚΟΨ"
]

PROTEST_RE = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(GREEK_PROTEST_STEMS)
)

en_words = ['protest', 'boycott', 'strike', 'walkout', 'march', 'travail', 'stoppage', 'workstop', 'picket', 'gather',
              'opposant', 'demonstr', 'rall', 'activis', 'stroke', 'mobilize']
pt_words = ['protest', 'manifesta', 'comício', 'boicot', 'ativis', 'greve', 'paralisaç', 'reuni', 'mobiliza']
de_en_words = [
    'protest', 'demonstr', 'ausstand', 'ausstände', 'stillstand', 'aufstand', 'aufruhr' 'aufrührer', 'randalierend',
    'massenprotest',
    'kundgebung', 'boykott', 'aktivis', 'streik', 'arbeitsniederlegung', 'revolte', 'marsch', 'märsche', 'block',
    'streikposten',
    'arbeitsstopp', 'versammel', 'schlag', 'besetz', 'mobilisier', 'insurrektion', 'rebellion', 'petition',
    'sitzblockade',
    'rall', 'boycott', 'activis', 'strike', 'strikes', 'walkout', 'stoppage', 'workstop', 'gather', 'stroke', 'mobilize'
]
root_words_bg = [
    'протест', 'протестирам', 'протестирал', 'протестирали', 'протестиращ', 'протестиращи', 'демонстрация',
    'демонстрации', 'демонстрант', 'демонстранти', 'митинг', 'митинги', 'манифестация', 'манифестации', 'активизъм',
    'активист', 'активисти',
    'стачка', 'стачки', 'забастовка', 'забастовки', 'прекъсване на работа', 'спиране на работа',
    'бойкот', 'бойкоти', 'бойкотирам', 'бойкотирал', 'бойкотирани', 'напускане на работа', 'неявяване на работа',
    'напуска', 'напуснал', 'напуснали', 'събиране',
    'събрали', 'съберат', 'събирам', 'събра', 'мобилизация', 'мобилизации', 'мобилизирам', 'мобилизиран',
    'мобилизирани',
    'гражданско неподчинение', 'солидарност', 'блокада', 'блокади', 'сид-ин', 'обсада',
]
pl_words = ['protest', 'demonstr', 'wiec', "bojkot", 'aktywi', 'strajk', "mobilizować", "zmobilizowany", 'zgromadze',
            'przerw'
            'zaprotest', "udar", "gromadzić się", "zgromadzeni", "opuszczenie pracy", "wstrzymanie pracy", 'zgromadze']

hr_words = ['protest', 'prosvjed', 'demonstra', 'bojkot', 'aktivi', 'štrajk', "napuštanje_rada", "obustava_rada",
            "prekid_rada", 'okupli', 'okuplj', 'udar', 'mobilizira']

ro_words = ['protest', 'protestâ', 'demonstra', 'demonstre', 'miting', 'boicot', 'activis', 'grev', 'oprire', 'aduna',
            'adună',
            'plecare în semn de protest', 'plecări în semn de protest', 'mobiliz', 'revolt', 'răscoal', 'insurecți',
            'rebeliun', 'revolt', 'petiți', 'blocad', 'blocând', 'sit-in', 'pichet', 'tabără de protest',
            'tabere de protest', 'dezobediență civilă', 'tulburare civilă', ]

de_words = [
    'protest', 'demonstr', 'ausstand', 'ausstände', 'stillstand', 'aufstand', 'aufruhr' 'aufrührer', 'randalierend',
    'massenprotest',
    'kundgebung', 'boykott', 'aktivis', 'streik', 'arbeitsniederlegung', 'revolte', 'marsch', 'märsche', 'block',
    'streikposten',
    'arbeitsstopp', 'versammel', 'schlag', 'besetz', 'mobilisier', 'insurrektion', 'rebellion', 'petition',
    'sitzblockade'
]

lt_words = ['protest', 'demonstra', 'mitinga', 'boikot', 'aktyvis', 'steik', 'išeiga', 'sustabdymas',
            'darbo_sustabdymas',
            'susirin', 'susirenk', 'mobilizuo']


root_words_nl = [
    'protest', 'geprotesteerd', 'demonstre', 'gedemonstre', 'bijeenkomst', 'boycot', 'geboycot', 'activis', 'stak',
    'stilstand', 'werkstaking','verzamel', 'bezet', 'mobilise', 'gemobiliseerd', 'relle','opstand', 'insurrectie', 'rebellie', 'revolte',
    'mars','petitie','blokkade', 'sit-in', 'posteren', 'aan het posten', 'geposteerd','kampement', 'kampementen','burgerlijke ongehoorzaamheid', 'burgerlijke onrust',
    'massa protest', 'massa protesten'
]
root_words_el_expanded = [
    'διαμαρτυρία', 'διαμαρτυρίες', 'διαμαρτυριών',
    'διαμαρτύρομαι', 'διαμαρτύρεται', 'διαμαρτύρονται', 'διαμαρτυρήθηκα', 'διαμαρτυρήθηκε', 'διαμαρτυρήθηκαν',
    'διαμαρτυρηθώ', 'διαμαρτυρηθεί', 'διαμαρτυρηθούν', 'διαδηλωτής', 'διαδηλωτές', 'διαδηλώτρια', 'διαδηλώτριες',
    'διαδηλωτή', 'διαδηλωτών', 'διαδήλωση', 'διαδηλώσεις', 'διαδηλώσεων', 'διαδηλώνω', 'διαδηλώνεις', 'διαδηλώνει',
    'διαδήλωνα', 'διαδηλώναμε', 'διαδήλωσα', 'διαδήλωσες', 'διαδήλωσε', 'διαδήλωσαν',
    'διαδηλώσω', 'διαδηλώσει', 'συγκέντρωση', 'συγκεντρώσεις', 'συγκεντρώσεων', 'μποϊκοτάζ', 'ακτιβισμός', 'ακτιβιστή',
    'ακτιβιστές',
    'ακτιβίστρια', 'ακτιβίστριες', 'απεργία', 'απεργίες', 'απεργών', 'απεργός', 'απεργοί', 'απεργού', 'απεργών',
    'απεργώ', 'απεργείς', 'απεργεί', 'απεργούν',
    'απεργούσα', 'απεργούσες', 'απεργούσε', 'απεργούσαν', 'απεργήσω', 'απεργήσει', 'απεργήσουν',
    'απεργούσαμε', 'απεργήσαμε', 'απεργήσατε', 'απεργήσανε', 'διακοπή', 'διακοπές', 'διακοπών',
    'διακόπτω', 'διακόπτεις', 'διακόπτει', 'διέκοψα', 'διέκοψε', 'διέκοψαν', 'διακόψω', 'διακόψει''συγκεντρώνομαι',
    'συγκεντρώνεσαι', 'συγκεντρώνεται', 'συγκεντρώνονται',
    'συγκεντρωνόμουν', 'συγκεντρωνόσουν', 'συγκεντρωνόταν', 'συγκεντρώθηκα', 'συγκεντρώθηκαν', 'συγκεντρωθήκαμε',
    'συγκεντρωθώ', 'συγκεντρωθεί', 'συγκέντρωση'

]
root_words_sv = [
    # Protest
    "protest", "protester", "protestera", "protesterade", "protesterat",

    # Demonstration
    "demonstration", "demonstrationer",
    "demonstrera", "demonstrerade", "demonstrerat",
    "demonstrationståg",

    # Riot
    "upplopp", "upploppet",

    # Revolt
    "uppror", "uppror",  # same in singular and plural

    # Rally (smaller, public)
    "folkmöte", "folkmöten",
    "torgmöte", "torgmöten",
    "massmöte", "massmöten",

    # Strike
    "strejk", "strejker",
    "strejka", "strejkade", "strejkat",
    "arbetstopp",         # literally "work‐stop"
    "arbetsnedläggelse", "arbetsnedläggningar",

    # Blockade
    "blockad", "blockader",
    "blockera", "blockerade", "blockerat",

    # Boycott
    "bojkott", "bojkotta", "bojkottade", "bojkottat",

    # Mobilization
    "mobilisera", "mobiliserade", "mobiliserat",

    # Activism
    "aktivism",
    "aktivist", "aktivister",

    # General words for "gathering"
    "samling", "samlingar",
    "samlas", "samlades", "samlats"
]

root_words_extended_cs = [
    'protest', 'odchod', 'úder', 'demonstr', 'shromáždění', 'bojkot', 'aktivis', 'stávk', 'zastavení', 'shromáždi',
    'obsadi',
    'zmobiliz', 'výtržn', 'povstání', 'vzpour', 'rebelie', 'revolt', 'pochod', 'petice', 'blokád', 'sit-in', 'piket',
    'tábořiště',
]
da_words = ['protest', 'demonstra', 'optog', 'aktivis', 'strejke', 'arbejdsnedlæggelse', 'arbejdsstop', 'boykot',
            'sammenkomst', 'salme', 'mobilisere', 'blokade', 'besættelse', 'sit-in']

et_words = ['protest', 'meeleavald', 'marss', 'kogun', 'aktivis', 'strei', 'tööseisak', 'lahku', 'boikot', 'mobiliseer',
            'üldstreik', 'kodanikuallumatus', 'blokaa', 'okkupatsioon', 'sit-in']

fi_words = ['mielenosoi', 'marssi', 'kokoontu', 'kansalaisaktivismi', 'aktivis', 'lakko', 'lakot', 'työtaistelu',
            'työnseisa', 'työseisaus',
            'boikot', 'mielenosoittaminen', 'suurlakko', 'yhdessäolo', 'kansalaistottelemattomuus']

fr_words = [
    'protest', 'manifest', 'rassemblement', 'boycott', 'activis', 'militant', 'grève',
    'débrayage', 'arrêt', 'rassemblement', 'coup'
]

it_words = [
    "protesta", "manifesta", "comizi", "comizi", "boicotta", "boicottare", "attivis", "attivista", "attivisti",
    "scioper", "uscita", "interruzione", "fermo_lavoro", "raduna", "colpo", "mobilizza"
]
hu_words = [
    'tiltakoz', 'demonstrá', 'felvonulá', 'gyűlés', 'összegyűl', 'aktivi', 'sztrájk',
    'munkabeszüntetés', 'munkahely elhagyása', 'kimenetel', 'bojkott', 'mozgósít',
    'általános sztrájk', 'polgári engedetlenség', 'szolidaritás', 'blokád', 'sit-in',
]

lv_words = [
    'protest', 'demonstr', 'gājiens', 'mītiņ', 'mītiņi', 'aktīvis', 'streik', 'darba pārtraukums',
    'darba pārtraukum', 'darba boikot', 'boikot', 'sapulcē',
    'pulcē', 'mobiliz', 'pilsoniskā nepakļaušanās', 'solidaritāte', 'blokāde', 'sit-in',
]
root_words_sk = [
    "protest", "demonstr", "demonštr", "pochod", "štrajk", "bojkot", "blokád", "blokova", "aktiviz", "aktivis",
]
root_words_da = [
    "protest", "demonstr", "optog","march", "siddestrejke", "strejke", "boykot", "blokade", "blokere", "aktivis", "mobiliser"]

en_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(en_words)
)

nl_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(root_words_nl)
)
pt_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(pt_words)
)
da_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(da_words)
)
et_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(et_words)
)
fi_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(fi_words)
)
fr_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(fr_words)
)
hu_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(hu_words)
)
de_en_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(de_en_words)
)
it_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(it_words)
)
lt_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(lt_words)
)
lv_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(lv_words)
)
ro_protest = re.compile(
    r"\b(?:%s)\w*\b" % "|".join(ro_words)
)

malta_url_rss = 'https://timesofmalta.com/sitemap_latest.xml'
poland_url_rss = 'https://www.rp.pl/sitemaps/news-sitemap.xml'
finland_url_rss = 'https://www.hs.fi/rss/custom/news-sitemap.xml'
croatia_url_rss = 'https://www.24sata.hr/news-sitemap.xml'
denmark_url_rss = 'https://www.bt.dk/sitemap.xml/news'
estonia_url_rss = 'https://www.postimees.ee/sitemap/news'
bulgaria_url_rss = 'https://www.standartnews.com/sitemap/latest_news.xml'
belgium_url_rss = 'https://www.lalibre.be/arc/outboundfeeds/sitemap-news/?outputType=xml'
netherlands_url_rss = 'https://www.volkskrant.nl/sitemaps/news.xml'
lithuania_url_rss = 'https://www.ve.lt/sitemap_news.xml'
czech_url_rss = 'https://www.blesk.cz/sitemap1-news.xml'
romania_url_rss = 'https://www.realitatea.net/share/news_sitemaps/sitemap.xml'
luxembourg_url_rss = "https://www.lessentiel.lu/sitemaps/de/news.xml"
germany_url_rss = 'https://www.welt.de/sitemaps/newssitemap/newssitemap.xml'
austria_url_rss = 'https://www.krone.at/google-news-sitemap.xml'
greece_url_rss = 'https://www.tanea.gr/wp-content/uploads/json/sitemap-news.xml'
italy_url_rss = 'https://www.repubblica.it/sitemap-n.xml'
france_url_rss = 'https://www.lemonde.fr/sitemap_news.xml'
portugal_url_rss = 'https://www.publico.pt/sitemaps/news.xml'
spain_url_rss = 'https://feeds.elpais.com/mrss-s/pages/ep/site/english.elpais.com/portada'
ireland_url_rss = 'https://www.irishtimes.com/arc/outboundfeeds/sitemap-news-index/latest/'  
hungary_url_rss = 'https://nepszava.hu/rss'
slovakia_url_rss = 'https://www.sme.sk/rss-title'
sweden_url_rss = 'https://www.gp.se/rss'
cyprus_url_rss = 'https://www.philenews.com/news.xml'  
latvia_url_rss = 'https://www.diena.lv/rss/'
