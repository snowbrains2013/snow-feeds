# SnowBrains NetNewsWire lead filter. Canonical copy: ~/SnowBrainsFeeds/nnw_scan.py (reads tags.json beside it).
# Usage: python3 nnw_scan.py DB.sqlite3 Subscriptions.opml HOURS leads.tsv   (writes dead_feeds.tsv to the current folder)
# Opens the database read-only and immutable; never writes to SnowBrainsFeeds.
# 2026-10-23a: region decided by the story's content (not the feed folder); outlet suffixes (" - Mammoth Times")
#   stripped before matching; events listings, finance-wire releases and sports results dropped; the same story
#   from several outlets collapsed into one row (new column also_in); Bing redirect links resolved to the
#   publisher; competitor rewrites flagged in freshness; prints how old NetNewsWire's newest article is.
# 2026-10-25a: client stories flagged ('CLIENT <name>' in matched, +4 score; client list in CLIENTS below, 2026-27 season);
#   first-to-open / lift-served opening stories flagged ('first-to-open', +3 when in the headline).
# 2026-11-10a: feed-factory items (feed address contains 'sbff-': DOT/511 closures, avalanche accidents, warnings) always count as leads,
#   scored by kind and treated as US/Canada; many more named passes and canyon roads; avalanche-control, chain-law and interlodge phrases;
#   ROAD also matches notch/gap/summit/parkway/interstate.
import sqlite3, sys, time, re, html, csv, json, os
import urllib.parse as up
import xml.etree.ElementTree as ET
VERSION = '2026-11-10a'
db, opml = sys.argv[1], sys.argv[2]
hours = float(sys.argv[3]) if len(sys.argv) > 3 else 24
out = sys.argv[4] if len(sys.argv) > 4 else 'leads.tsv'
feeds = {}
def walk(node, folder):
    for o in node.findall('outline'):
        if o.get('xmlUrl'): feeds[o.get('xmlUrl')] = (folder, o.get('title') or o.get('text'))
        else: walk(o, o.get('title') or o.get('text'))
walk(ET.parse(opml).getroot().find('body'), '(top)')

STRONG = (
 # avalanches, skiers, resorts
 r"avalanch|lawine|valanga|雪崩|skier|skieuse|esquiador|snowboarder|ski resort|ski area|skigebiet|station de ski|domaine skiable|stazione sciistica|comprensorio sciistico|estaci[oó]n de esqu|スキー場|"
 # lifts
 r"chairlift|chair lift|gondola|\bgondel|tram |cable car|ropeway|seilbahn|sessellift|\bskilift|télésiège|telesiege|téléphérique|t[ée]l[ée]cabine|remont[ée]es? m[ée]caniques?|seggiovia|funivia|cabinovia|telesilla|ski lift|lift (?:accident|fall|evacuat|derail|detach)|ゴンドラ|リフト|"
 # backcountry
 r"ski patrol|heli-?ski|cat-?ski|backcountry|skitour|ski touring|ski mountaineer|skialp|off-piste|hors-piste|fuoripista|tree well|snowmobiler|"
 # industry
 r"vail resorts|alterra|ikon pass|epic pass|indy pass|mountain collective|season pass|lift ticket|aspen skiing|powdr|boyne|deer valley|jackson hole|palisades tahoe|mammoth mountain|whistler|rob katz|kirsten lynch|"
 # forecasts and snow
 r"farmers'? almanac|old farmer|winter outlook|winter forecast|snow forecast|snowiest|winter storm|polar vortex|atmospheric river|\bnoaa\b|\benso\b|climate prediction center|"
 r"la niña|la nina|el niño|el nino|snowpack|snowfall|first snow|blizzard|water year|"
 # named mountain passes and canyon roads (closures are SnowBrains stories; missed Tioga/Guardsman/Trail Ridge 10/2026)
 r"tioga pass|sonora pass|trail ridge road|guardsman pass|independence pass|going-to-the-sun|beartooth highway|teton pass|little cottonwood|big cottonwood|loveland pass|berthoud pass|red mountain pass|wolf creek pass|monarch pass|snoqualmie pass|donner pass|rogers pass|sea-to-sky|"
 # more named passes, notches and canyon roads + avalanche-control and chain-law phrases (11/10/26)
 r"avalanche control|avalanche mitigation|avalanche warning|avalanche watch|interlodge|chain controls?|chain law|traction law|"
 r"stevens pass|white pass|chinook pass|cayuse pass|north cascades highway|washington pass|lookout pass|lolo pass|galena summit|togwotee pass|snowy range|"
 r"beartooth pass|cameron pass|rabbit ears pass|vail pass|hoosier pass|fremont pass|lizard head pass|coal bank pass|molas pass|cottonwood pass|kebler pass|"
 r"mcclure pass|kenosha pass|echo summit|carson pass|ebbetts pass|monitor pass|luther pass|mount rose highway|mt\.? rose highway|donner summit|tioga road|"
 r"siskiyou summit|santiam pass|willamette pass|mckenzie pass|coquihalla|kootenay pass|kicking horse canyon|allison pass|icefields parkway|highwood pass|"
 r"smugglers'? notch|kancamagus|pinkham notch|franconia notch|crawford notch|appalachian gap|lincoln gap|whiteface memorial|thompson pass|turnagain pass|"
 r"hatcher pass|seward highway|parleys canyon|provo canyon|logan canyon|glenwood canyon|eisenhower tunnel|"
 # mountaineering
 r"everest|\bk2\b|denali|mountaineer|alpinist|\bclimbers?\b|bergsteiger|alpinista|glacier collapse|"
 # accidents in other languages
 r"pistenunfall|skiunfall|skifahrer|sciator|sciatric|skieur|accident de ski|incidente.{0,10}sci|スキー|スノーボード|遭難|"
 # rescue
 r"search and rescue|\bsar\b|mountain rescue|bergrettung|secours en montagne|soccorso alpino|"
 # resort investment (ski-specific phrases only; generic money words live in BIZ)
 r"snowmaking|terrain expansion|lift replacement|new (?:chair)?lifts?\b|base lodge|summit lodge")
CTX = r"ski|snow|mountain|resort|alpine|piste|glacier|summit|\blift|climb|schnee|neige|neve|berg|montagne|montagna|雪|山"
# Business/money words. A resort named in the HEADLINE plus one of these flags the story even with no
# ski/snow word (e.g. "Killington & Pico Detail $280 Million Capital Improvement Plan", missed 10/16/26).
BIZ = re.compile(r"capital (?:improvement|plan|project|investment)|\$\s?\d[\d.,]*\s?(?:million|billion|mn|bn|m\b|b\b)|\b\d[\d.,]*\s?(?:million|billion)\b|"
 r"invest|expan|upgrad|renovat|redevelop|master plan|acqui|\bsale\b|\bsold\b|\bbuys?\b|purchas|new owner|ownership|merger|"
 r"lawsuit|\bsue[sd]?\b|bankrupt|closure|clos(?:e|es|ing)\b|reopen|approv|permit|propos|announc|unveil|\bplans?\b|lodge|terrain|\blifts?\b", re.I)
GENERIC = r"\b(?:died|dies|dead|death|killed|fatal|missing|rescued|injured|arrest|charged|lawsuit|sued|settlement|closure|closing|bankrupt|sold|acquir|tödlich|verunglückt|vermisst|mort|décédé|disparu|morto|deceduto|disperso|死亡|行方不明)"
# Always noise, anywhere in the story. "Colorado Avalanche Information Center" is NOT noise.
NOISE = re.compile(r"(?:jet ?ski|snow ?crab|snow white|\bavalanche (?:vs|rival|pick|game|beat|lose|win|top|sign)|"
 r"avalanche de (?:buts|critiques|mauvaises|réactions|commentaires)|(?:formato|una) valanga|valanga di (?:gol|voti|soldi)|lawine (?:an|von) (?:kritik|anfragen))", re.I)
# Hockey/betting words only count as noise when the story is about "Avalanche" (the NHL team), not when they appear elsewhere.
HOCKEY = re.compile(r"\b(?:vs\.?|stars|nhl|hockey|mackinnon|makar|necas|rantanen|blackwood|fantasy|betting|odds|goalie|goals?|overtime|power play|puck|shutout|assists?|rink|defen[cs]e|lineup|roster|maple leafs|avax|crypto|token|blockchain)\b", re.I)
# The NHL team. Only noise when there is no real snow/avalanche context: "Colorado avalanche researchers
# using new tools to forecast how far avalanches could travel" (CAIC, 10/19/26) was being thrown out.
NHL_TEAM = re.compile(r"Colorado Avalanche(?! Information)")
SNOW_AVAL = re.compile(r"\bsnow|\bski(?:s|er|ers|ing)?\b|\bslides?\b|buried|backcountry|\bslopes?\b|caic|avalanches\b|avalanche (?:danger|risk|center|centre|warning|conditions|terrain|safety|researchers?|forecast)|runout", re.I)
# Mountain roads: a pass/canyon/highway closing or reopening counts as a lead when there's mountain/snow context.
ROAD = re.compile(r"\b(?:pass|canyon|highway|hwy|road|route|notch|gap|summit|parkway|interstate|i-\d+|sr[ -]?\d+)\b[^.!?]{0,40}\b(?:clos(?:e|es|ed|ing|ure)|reopen\w*)\b|\b(?:clos(?:e|es|ed|ing|ure)|reopen\w*)\b[^.!?]{0,40}\b(?:pass|canyon|notch|summit)\b", re.I)
METROS = {'Anchorage','Beijing','Boston','Boulder','Calgary','Christchurch','Denver','Geneva','Las Vegas','Los Angeles','Milan','New York City','Portland','Reno',
          'Salt Lake City','San Francisco','Santiago','Sapporo','Seattle','Spokane','Tokyo','Vancouver','Innsbruck','Mendoza','Burlington','Ogden','Missoula','Bozeman'}
TOWNBIZ = re.compile(r"\b(?:to close|closes|closing|closed for good|shut(?:s|ting)? down|for sale|sold|bankrupt\w*|lawsuit|sues|housing|short-term rental|new owner|reopen\w*)\b", re.I)
NA_WORDS = re.compile(r"\b(?:Alaska|US|Tioga|Sonora Pass|Monterey|Mother Lode|Nevada County|Teton Pass|Guardsman|Independence Pass|Trail Ridge|Loveland Pass|Berthoud|Red Mountain Pass|Wolf Creek Pass|Stevens Pass|Rogers Pass|Sea-to-Sky|Coquihalla|Ohio|Indiana|Illinois|Iowa|New Jersey|West Virginia|Connecticut|Rhode Island|Maryland|Delaware|Kentucky|Tennessee|South Dakota|North Dakota|Nebraska|Texas|Oklahoma|Missouri|Kansas|Arkansas|Nova Scotia|New Brunswick|Newfoundland|Manitoba|Saskatchewan|Northwest Territories|Nunavut|Bow Valley|Lake Louise|Sunshine Village|Norquay|Sun Peaks|Revelstoke|Kicking Horse|Fernie|Big White|Silver Star|Red Mountain|Panorama|Mont[- ]Tremblant|Blue Mountain|Mont-Sainte-Anne|Le Massif|Lake Placid|Whiteface|Gore Mountain|Stowe|Killington|Sugarbush|Jay Peak|Sugarloaf|Sunday River|Loon|Bretton Woods|Cannon|Wildcat|Attitash|Okemo|Stratton|Mount Snow|Camelback|Seven Springs|Snowshoe|Steamboat|Telluride|Breckenridge|Keystone|Copper Mountain|Winter Park|Crested Butte|Arapahoe Basin|A-Basin|Loveland|Eldora|Monarch|Wolf Creek|Purgatory|Durango|Silverton|Taos|Santa Fe|Angel Fire|Snowbowl|Big Sky|Bridger Bowl|Whitefish|Sun Valley|Bogus Basin|Schweitzer|Tamarack|Grand Targhee|Teton|Alta|Snowbird|Brighton|Solitude|Powder Mountain|Snowbasin|Brian Head|Deer Valley|Canyons Village|Heavenly|Kirkwood|Northstar|Sugar Bowl|Mt\.? Rose|Mount Rose|Sky Tavern|Diamond Peak|Mammoth Lakes|June Mountain|China Peak|Bear Mountain|Big Bear|Mount Waterman|Mt\.? Waterman|Mount Baldy|Mt\.? Baldy|Mount Shasta|Mt\.? Shasta|Mount Hood|Mt\.? Hood|Timberline|Mt\.? Bachelor|Crystal Mountain|Stevens Pass|Snoqualmie|Mt\.? Baker|Mission Ridge|Lee Canyon|Alyeska|Girdwood|Bend|Truckee|Olympic Valley|Central Valley|Sierra Nevada|Wasatch|Cottonwood|Front Range|Flat Tops|Routt|Summit County|Eagle County|Pitkin|Gunnison|Siskiyou|Placer County|Washoe|Arizona|California|Colorado|Idaho|Maine|Massachusetts|Michigan|Minnesota|Montana|Nevada|New Hampshire|New Mexico|New York|North Carolina|Oregon|Pennsylvania|Utah|Vermont|Virginia|Washington|Wisconsin|Wyoming|Tahoe|Sierra|Rockies|Cascades|Adirondack|Catskill|U\.S\.|USA|American|Canada|Canadian|B\.C\.|British Columbia|Alberta|Quebec|Québec|Ontario|Yukon|Banff|Whistler|Yellowstone|Yosemite|Glacier National Park|Grand Teton|Rainier|Denali|Rocky Mountain National Park|Going-to-the-Sun|Beartooth|I-70|I-80|Denver|Salt Lake|Park City|Boulder|Reno|Seattle|Spokane|Bozeman|Missoula|Jackson Hole|Aspen|Vail|Mammoth|Anchorage|Vancouver|Calgary|Montreal|Montréal|Toronto|Ottawa|Squamish|Cottonwood)\b")
AVAL_WORD = re.compile(r"\bavalanches?\b", re.I)
# A story using the word "avalanche" must also show some real-avalanche context, or it's the NHL team, crypto or figurative.
AVAL_CTX = re.compile(r"ski|snow|mountain|alpine|alps|piste|glacier|summit|peak|slope|climb|backcountry|slide|buried|rescue|secours|soccorso|"
 r"\brock|\bice\b|debris|mine|flood|landslide|victim|dead|died|kill|death|morts?\b|décès|hélicopt|helicopt|sherpa|himala|beacon|safety|"
 r"warning|danger|forecast|risk|neige|montagne|schnee|berg|neve|montagna|雪|山", re.I)
# Italian "a valanga" = "by a landslide" (sports scores), never a real avalanche.
NOISE2 = re.compile(r"\ba valanga\b|valanga di (?:insulti|richieste|ordini|messaggi)|bollette\b.{0,40}valanga", re.I)
# Event calendars and finance-wire releases that local Google News feeds pick up.
EVENTS = re.compile(r"\bEvents\s*[-–:]\s|^(?:Calendar|Events?|What's happening|Things to do)\b", re.I)
FINWIRE = re.compile(r"Reverse Stock Split|Senior Notes|Notes Offering|Nasdaq (?:Compliance|Listing)|Announces (?:Pricing|the Resignation|Resignation|Quarterly|Dividend)|"
 r"Quarterly (?:Cash )?Dividend|Earnings Call|Investor Day|\bIPO\b pricing", re.I)
SPORTS = re.compile(r"\b(?:football|soccer|basketball|NBA|NFL|MLS|NHL|MLB|volleyball|baseball|softball|preseason polls?|playoffs?|quarterback|touchdown|"
 r"Champions League|Conference League|Europa League|Coppa Italia|Serie A|Premier League|Copa|campeonato|fútbol|futbol|calcio|"
 r"Mountaineers (?:top|beat|fall|lose|win)|WVU|Class A{1,2} football|Big Sky (?:Conference|preseason|football|women|men))\b", re.I)
SKI_CORE = re.compile(r"\bski|snowboard|chairlift|gondola|snowpack|snowfall|\bsnow\b|backcountry|avalanche (?:danger|center|centre|forecast|warning|risk)|lift ticket|season pass|"
 r"esquí|esqui|\bsci\b|sciator|skieur|neige|schnee|スキー", re.I)
# Outlet names that contain lead words ("Revelstoke Mountaineer", "Mountain Express") are removed before matching.
OUTLETS = re.compile(r"Revelstoke Mountaineer|Idaho Mountain Express|Mountain Democrat|Mountain Times|Mammoth Times|Mountain Xpress|Mountain Mail|"
 r"Mountain Mail|Mountaineer(?:s)? (?:Athletics|Football)|Colorado Avalanche(?= (?:vs|beat|game|top|fall|lose|win))", re.I)
# International evidence: countries, regions and big resorts outside North America.
INTL_WORDS = re.compile(r"\b(?:France|French|Switzerland|Swiss|Austria|Austrian|Ital(?:y|ian)|German(?:y)?|Bavaria\w*|Tyrol\w*|Tirol\w*|Valais|Wallis|Graubünden|"
 r"Alps|Alpes|Alpen|Alpi|Dolomit\w+|Pyren\w+|Pyrén\w+|Andorra|Spain|Spanish|Norw\w+|Swed\w+|Finland|Finnish|Iceland\w*|Scotland|Scottish|"
 r"Brit(?:ain|ish)|England|English|Wales|Welsh|Ireland|Slovenia\w*|Czech\w*|Poland|Polish|Slovak\w*|Bulgaria\w*|Romania\w*|Georgian|"
 r"Japan\w*|Hokkaido|Niseko|Hakuba|Nagano|Korea\w*|China|Chinese|Mongolia\w*|Nepal\w*|Himalaya\w*|Everest|Karakoram|Pakistan\w*|India\b|Indian|Kashmir|Tibet\w*|"
 r"Chile\w*|Argentin\w*|Andes|Portillo|Patagonia\w*|Bariloche|Mendoza|Peru\w*|Bolivia\w*|New Zealand|Kiwi|Queenstown|Wanaka|Australia\w*|Thredbo|Perisher|"
 r"Russia\w*|Ukrain\w*|Turk\w+|Leban\w+|Morocc\w+|Kazakh\w*|Uzbek\w*|Iran\w*|Chamonix|Zermatt|Verbier|Courchevel|Méribel|Val d'Isère|Tignes|St\.? Anton|"
 r"Kitzb\w+|Sölden|Hintertux|Cortina|Livigno|Bormio|Cervinia|Saalbach|Ischgl|Lech|Engelberg|Davos|St\.? Moritz|Laax|Garmisch|Mont Blanc|Matterhorn|Eiger|Jungfrau)\b")
QC_WORDS = re.compile(r"Québec|Quebec|Montréal|Montreal|Laurentides|Laurentians|Charlevoix|Estrie|Gaspésie|Saguenay|Outaouais|Mont-Sainte-Anne|Tremblant|Le Massif|"
 r"Stoneham|Bromont|Sutton|Orford|Mont-Saint-Sauveur|Val Saint-Côme|Mont Blanc \(Québec\)|Canada|canadien", re.I)
NON_EN = re.compile(r"[぀-ヿ一-鿿]|\b(?:les|des|une|dans|avec|pour|sur la|der|die|und|mit|nach|einem|degli|della|nella|sulla|con il|los|las|una|para|tras|según)\b")
RESCUE = re.compile(r"rescue|climber|summit|glacier|crevasse|avalanch|bergrett|secours|soccorso|stranded|missing hiker|search and rescue", re.I)
COMPETITORS = ('unofficialnetworks.com', 'powder.com', 'snowboarder.com', 'outsideonline.com', 'skimag.com', 'adventuresportsnetwork.com',
               'mensjournal.com', 'onthesnow.com', 'the-ski-guru.com', 'newschoolers.com', 'skiing.com', 'snow-forecast.com')
COMPETITOR_NAMES = re.compile(r"\b(?:Unofficial Networks|POWDER Magazine|SNOWBOARDER Magazine|Men's Journal|Outside Magazine|Outside Online|SKI Magazine|OnTheSnow|Adventure Sports Network)\s*$", re.I)
def real_url(u):
    # Bing News redirect -> the publisher's link (Google News links can't be decoded offline)
    if u and 'bing.com/news/apiclick' in u:
        q = up.parse_qs(up.urlparse(u).query).get('url')
        if q: return q[0]
    return u
def core_title(t):
    # "Headline - Outlet" -> "Headline" (Google News / Bing append the outlet; it caused false matches)
    return re.sub(r"\s+[-–|]\s+[^-–|]{2,70}$", "", t).strip()
strong_re, ctx_re, gen_re = re.compile(STRONG, re.I), re.compile(CTX, re.I), re.compile(GENERIC, re.I)
# SnowBrains clients, 2026-27 season (Steven, 10/25/26). Name shown in 'matched' -> regex. Edouard is left out:
# the name alone is too ambiguous to match safely.
CLIENTS = [(n, re.compile(r, f)) for n, r, f in [
    ('Big White', r"\bBig White\b", 0), ('Mount Bohemia', r"\b(?:Mount|Mt\.?) Bohemia\b", re.I), ('Ikon Pass', r"\bIkon\b", 0),
    ('SPOOGE', r"\bSPOOGE\b|\bSpooge (?:sunscreen|ski|skin|eye)", 0), ('ALPINR', r"\bALPINR\b", re.I),
    ('SkiBig3', r"\bSki ?Big ?3\b|\bBanff Sunshine\b|\bSunshine Village\b|\bLake Louise (?:Ski|Mountain)|\b(?:Mt\.?|Mount) Norquay\b|\bNorquay\b", re.I),
    ('Naked Nutrition', r"\bNaked Nutrition\b", re.I), ('RED Resort', r"\bRED Mountain\b|\bRED Resort\b|\bRed Mountain Resort\b|\bRossland\b", 0),
    ('Silverton', r"\bSilverton Mountain\b", re.I), ('Nikwax', r"\bNikwax\b", re.I), ('Poclink', r"\bPoclink\b", re.I),
    ('Kicking Horse', r"\bKicking Horse\b(?! (?:Canyon|Pass|River))", re.I), ('Mammoth', r"\bMammoth (?:Mountain|Lakes)\b", re.I),
    ('Bachelor', r"\b(?:Mt\.?|Mount) Bachelor\b", re.I), ('Flowline', r"\bFlowline\b", 0), ('Stowe', r"\bStowe\b", 0),
    ('Palisades Tahoe', r"\bPalisades Tahoe\b", re.I), ('Monarch Mountain', r"\bMonarch (?:Mountain|Ski)", re.I),
    ('Maison Sport', r"\bMaison Sport\b", re.I), ('Deer Valley', r"\bDeer Valley\b", re.I), ('Arms of Andes', r"\bArms of (?:the )?Andes\b", re.I),
    ('Ski California', r"\bSki California\b", re.I),
    ('Mount Washington (BC)', r"\b(?:Mount|Mt\.?) Washington Alpine\b|\b(?:Mount|Mt\.?) Washington\b(?=[^.]{0,120}(?:Vancouver Island|Comox|Courtenay|B\.?C\.?\b|British Columbia))", re.I),
    ('Jackson Hole Mountain Resort', r"\bJackson Hole Mountain Resort\b|\bJHMR\b|\bJackson Hole\b", 0)]]
# first lift-served skiing / race to open (Steven's rule, 10/25/26)
OPENING = re.compile(r"lift-served|first (?:to open|chairs?|lift rides?|turns|tracks of the season)|race to open|opening day|"
                     r"(?:opens?|opening|opened) (?:for|its) (?:the )?(?:\d{4}-\d{2,4} )?(?:ski )?season|first (?:ski )?(?:resort|area|hill) (?:in|to)|"
                     r"(?:delays?|pushes?|postpones?) (?:its )?opening", re.I)
def clean(s): return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', s or ''))).strip()

# Canonical tags (optional): tags.json beside this script.
tag_cs, tag_ci, tag_meta = {}, {}, {}
tp = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tags.json')
if os.path.exists(tp):
    for t in json.load(open(tp, encoding='utf-8'))['tags']:
        tag_meta[t['tag']] = t
        if t['type'] == 'Topic':
            if len(t['tag']) >= 6: tag_ci[t['tag'].lower()] = t['tag']   # topic: canonical name only
            continue
        tag_cs[t['tag']] = t['tag']                                       # entity: exact case, whole word
        for a in t['aliases']:
            if len(a) >= 5: tag_ci.setdefault(a.lower(), t['tag'])
def alt(keys, flags=0):
    if not keys: return None
    return re.compile(r'(?<!\w)(?:' + '|'.join(re.escape(k) for k in sorted(keys, key=len, reverse=True)) + r')(?!\w)', flags)
TAG_CS, TAG_CI = alt(tag_cs), alt(tag_ci, re.I)
def find_tags(blob):
    found = []
    if TAG_CS: found += [tag_cs[m] for m in TAG_CS.findall(blob)]
    if TAG_CI: found += [tag_ci[m.lower()] for m in TAG_CI.findall(blob) if m.lower() in tag_ci]
    return list(dict.fromkeys(found))

c = sqlite3.connect(f'file:{db}?mode=ro&immutable=1', uri=True)
since = time.time() - hours * 3600
# Window is by ARRIVAL in NetNewsWire, not publish date: Bing/Google News items often arrive hours after
# their publish time and were slipping past short windows. Publish date is still capped at PUB_MAX_DAYS
# so resurfaced old stories stay out.
PUB_MAX_DAYS = float(os.environ.get('NNW_PUB_MAX_DAYS', 4))
oldest_pub = time.time() - PUB_MAX_DAYS * 86400
rows = c.execute("""select a.feedID, a.title, coalesce(a.externalURL,a.url), a.summary, a.contentText, a.contentHTML,
  a.datePublished, s.dateArrived from articles a join statuses s using(articleID)
  where coalesce(s.dateArrived, a.datePublished) > ? and coalesce(a.datePublished, s.dateArrived) > ?""", (since, oldest_pub)).fetchall()
groups, leads = {}, []
for feedID, title, url, summ, text, chtml, pub, arr in rows:
    via_agg = any(a in (url or '') for a in ('news.google.com', 'bing.com/news', 'msn.com'))
    url = real_url(url)
    # WSDOT's pass-conditions feed re-posts all 16 passes whenever one changes: only closures, avalanche work and chain rules are leads
    if 'MountainPassConditions' in (feedID or '') and not re.search(r'clos|avalanche|required|prohibited', f"{title} {summ or text or chtml or ''}", re.I): continue
    ff = 'sbff-' in (feedID or '')   # SnowBrains feed factory: already filtered to closures, avalanche accidents and warnings
    # only aggregator headlines carry a " - Outlet" suffix; Krone etc. use " - " inside real headlines
    full_title = clean(title); title = core_title(full_title) if via_agg else full_title; body = clean(summ or text or chtml)[:600]
    body = body.replace(full_title, ' ').replace(title, ' ')   # GN repeats the headline as the summary
    blob = OUTLETS.sub(' ', f"{title} {body}"); title = OUTLETS.sub(' ', title).strip() or title
    if not title or 'snowbrains' in (feedID or '').lower() or 'snowbrains' in (url or '').lower() or 'snowbrains' in blob.lower(): continue
    if NOISE.search(blob) or NOISE2.search(blob): continue
    if EVENTS.search(full_title): continue
    if FINWIRE.search(title) and not strong_re.search(title): continue
    if SPORTS.search(blob) and not (SKI_CORE.search(blob) or RESCUE.search(blob)): continue
    if not ff and NHL_TEAM.search(blob) and (HOCKEY.search(blob) or not SNOW_AVAL.search(blob)): continue
    if not ff and AVAL_WORD.search(blob) and (HOCKEY.search(blob) or not AVAL_CTX.search(blob)):
        # the hockey/crypto/figurative test only removes stories the word "avalanche" brought in
        if not strong_re.search(AVAL_WORD.sub(' ', blob)): continue
    hits = {m.group(0).lower().strip() for m in strong_re.finditer(blob)}
    score = 2 * len(hits) + (3 if strong_re.search(title) else 0)
    if ff:
        hits = set(hits) | {'feed-factory'}
        score += 8 if re.search(r'FATALITY|ACCIDENT|INVOLVEMENT|AVALANCHE', title) else 5 if re.search(r'\bCLOSED\b|WARNING|DANGER|REOPENED', title) else 2
    tags = find_tags(blob)
    has_ctx = bool(ctx_re.search(blob)) or ff
    title_tags = set(find_tags(title))
    def ok(t):
        f = tag_meta.get(t, {}).get('flag')
        if f == 'any' or (f == 'ctx' and has_ctx): return True
        # resort named in the headline + a business/money word anywhere = enough context on its own
        return f == 'ctx' and tag_meta.get(t, {}).get('type') == 'Resort' and t in title_tags and bool(BIZ.search(blob))
    flag_tags = [t for t in tags if ok(t)]
    if not hits and gen_re.search(title) and has_ctx: hits, score = {'generic+ctx'}, 1
    if ROAD.search(title) and has_ctx:
        hits = set(hits) | {'mountain-road'}; score += 3
    for t in title_tags:
        m = tag_meta.get(t, {})
        # ski town or pass/road named in the headline + business/closure word (e.g. a Tahoe restaurant closing, a pass closing)
        if m.get('type') != 'Location': continue
        if m.get('sub') == 'Pass & Road' and ROAD.search(title):
            hits = set(hits) | {'pass/road closure'}; score += 2; flag_tags.append(t)
        elif m.get('sub') == 'Town' and t not in METROS and TOWNBIZ.search(title) and has_ctx:
            hits = set(hits) | {'ski-town business'}; score += 2; flag_tags.append(t)
    if flag_tags:
        score += len(flag_tags) + (2 if any(t in title for t in flag_tags) else 0)
        if not hits: hits = {'tag'}
        if any(t in title_tags and tag_meta.get(t, {}).get('type') == 'Resort' for t in flag_tags) and BIZ.search(title):
            hits.add('resort+biz'); score += 2   # resort news in the headline: investment, sale, lawsuit, closure
    clients = [n for n, rx in CLIENTS if rx.search(blob)]
    if clients and (has_ctx or any(n in ('SPOOGE', 'ALPINR', 'Naked Nutrition', 'Nikwax', 'Poclink', 'Maison Sport', 'Arms of Andes') for n in clients)):
        hits = set(hits) | {'CLIENT ' + n for n in clients}; score += 4
    if OPENING.search(blob) and has_ctx:
        hits = set(hits) | {'first-to-open'}; score += 3 if OPENING.search(title) else 1
    if not hits: continue
    fresh = 'CHECK DATE'
    m = re.search(r'/(20\d\d)[/-](\d\d)(?:[/-](\d\d))?[/-]', url or '') or re.search(r'(20\d\d)(\d\d)(\d\d)', url or '')
    if m:
        try:
            if time.mktime((int(m.group(1)), int(m.group(2)), int(m.group(3) or 28), 23, 59, 0, 0, 0, -1)) < since - 2 * 86400: continue
            fresh = 'url-dated'
        except (OverflowError, ValueError): pass
    if 'news.google.com' in (url or '') or 'bing.com' in (url or '') or 'msn.com' in (url or ''): fresh = 'CHECK DATE (aggregator)'
    host = (up.urlparse(url or '').hostname or '').replace('www.', '')
    if any(host == d or host.endswith('.' + d) for d in COMPETITORS) or COMPETITOR_NAMES.search(full_title):
        fresh = 'COMPETITOR: trace original + date'
    folder, ftitle = feeds.get(feedID, ('?', feedID))
    # region: decided by what the story is about. Feed folder is only a tie-breaker (the "GN Québec" feed returns
    # French resorts; "La Tercera" returns Montreal soccer).
    na_ev = intl_ev = 0
    for t in tags:
        m = tag_meta.get(t, {})
        w = 2 if t in title_tags else 1
        if m.get('country') in ('USA', 'Canada') or m.get('sub') in ('North America', 'US State', 'Canadian Province/Territory', 'Avalanche Center'): na_ev += w
        elif m.get('country') or m.get('sub') in ('International',): intl_ev += w
    na_ev += 2 * len(NA_WORDS.findall(title)) + len(NA_WORDS.findall(body)) + (3 if ff and 'news-search' not in feedID else 0)
    intl_ev += 2 * len(INTL_WORDS.findall(title)) + len(INTL_WORDS.findall(body))
    if NON_EN.search(title):
        if QC_WORDS.search(blob): na_ev += 2
        else: intl_ev += 2
    if na_ev == intl_ev:
        if 'Local:' in folder and 'Québec' not in ftitle and 'Quebec' not in ftitle: na_ev += 1
        elif folder.startswith('SB · ') and any(k in folder for k in ('Europe', 'Japan', 'ANZ', 'Himalaya', 'Police', 'Intl')): intl_ev += 1
    region = 'US/Canada' if na_ev > intl_ev else ('International' if intl_ev > na_ev else '?')
    key = re.sub(r'[^a-z0-9\u3040-\u30ff\u4e00-\u9fff]+', '', title.lower())[:60]
    outlet = (full_title[len(title):].strip(' -–|') if full_title.startswith(title) else '') or host or ftitle
    row = [score, time.strftime('%m/%d %H:%M', time.gmtime(pub or arr)), folder, ftitle, full_title, url,
           ', '.join(sorted(hits))[:80], body[:200], fresh, '', region, '; '.join(tags[:8])]
    groups.setdefault(key, []).append((row, outlet))
# same story from several outlets -> one row: best score, prefer a publisher link over an aggregator link,
# earliest timestamp kept in published_utc; the other outlets go in also_in
agg = lambda u: any(a in (u or '') for a in ('news.google.com', 'bing.com', 'msn.com', 'yahoo.com'))
for key, items in groups.items():
    items.sort(key=lambda it: (-it[0][0], agg(it[0][5]), it[0][1]))
    best = list(items[0][0])
    best[1] = min(it[0][1] for it in items)
    others = list(dict.fromkeys(o for _, o in items[1:] if o and o != items[0][1]))
    if others: best[9] = f"{len(items) - 1}: " + ', '.join(others[:4])[:120]
    leads.append(tuple(best))
leads.sort(key=lambda r: (-r[0], r[1]))
with open(out, 'w', newline='') as f:
    w = csv.writer(f, delimiter='\t')
    w.writerow(['score','published_utc','folder','feed','title','url','matched','snippet','freshness','also_in','region','tags'])
    w.writerows(leads)
have = {r[0] for r in c.execute("select distinct feedID from articles")}
dead = sorted((fo, t, u) for u, (fo, t) in feeds.items() if u not in have)
with open('dead_feeds.tsv', 'w', newline='') as f: csv.writer(f, delimiter='\t').writerows([('folder','title','url')] + dead)
newest = c.execute("select max(dateArrived) from statuses").fetchone()[0] or 0
age_h = (time.time() - newest) / 3600
from collections import Counter
reg = Counter(r[10] for r in leads)
print(f"v{VERSION} | in window: {len(rows)} | leads: {len(leads)} (US/Canada {reg['US/Canada']}, International {reg['International']}, ? {reg['?']})"
      f" | tags loaded: {len(tag_meta)} | feeds: {len(feeds)} | never fetched: {len(dead)}")
print(f"NetNewsWire newest article arrived {age_h:.1f} h ago" + ("  <-- WARNING: NetNewsWire has not refreshed recently" if age_h > 10 else ""))
