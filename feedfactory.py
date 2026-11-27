#!/usr/bin/env python3
"""SnowBrains feed factory: free road-closure, avalanche and news-search feeds for NetNewsWire and the cloud scan.

Polls ~90 keyless public sources (state/provincial 511 systems, DOT data, avalanche-center APIs, park road
status, news searches), keeps only what matters for ski country (closures, reopenings, avalanche work,
slides, accidents, warnings), remembers what it has already seen, and writes:
  sbff-roads-us-west.xml  sbff-roads-us-rockies.xml  sbff-roads-us-east.xml  sbff-roads-canada.xml
  sbff-avalanche.xml  sbff-parks.xml  sbff-native.xml  sbnews-search.xml (news searches, incl. relayed Google News)
  all.json (everything, newest first)  status.json (health of every source)  index.html

  python3 feedfactory.py run   [--out DIR]            one pass (use this from cron / GitHub Actions)
  python3 feedfactory.py serve [--out DIR] [--port 8511] [--every 600]   run forever + local web server
  python3 feedfactory.py test  [--only id,id]         fetch and print, write nothing
  python3 feedfactory.py list                         list sources
  python3 feedfactory.py opml  BASE_URL               print an OPML for NetNewsWire
Standard library only (works with macOS /usr/bin/python3 and on GitHub Actions).
"""
import concurrent.futures as cf, email.utils, gzip, hashlib, html, json, math, os, re, sys, threading, time
import urllib.error, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from xml.sax.saxutils import escape

VERSION = "2026-11-20c"
UA = "Mozilla/5.0 (compatible; SnowBrains-feedfactory/%s; +https://snowbrains.com)" % VERSION
HERE = os.path.dirname(os.path.abspath(__file__))
KEEP_DAYS, BOOT_HOURS, GONE_HOURS = 10, 48, 36
NOW = time.time

# ------------------------------------------------------------------ helpers
def fetch(url, data=None, headers=None, timeout=45):
    h = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h)   # always identifies itself honestly; a site that refuses it is left alone
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
        if r.headers.get("Content-Encoding") == "gzip" or b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        return b

def fetch_json(url, **kw):
    return json.loads(fetch(url, **kw).decode("utf-8", "replace"))

def strip(s):
    s = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", s or "", flags=re.S | re.I)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s))).strip()

def short(t, n=170):
    t = re.sub(r"\s+", " ", t or "").strip()
    first = re.split(r"(?<=[.!])\s+(?=[A-Z])", t)[0]
    if len(first) >= 45: t = first
    return t if len(t) <= n else t[: n - 1].rsplit(" ", 1)[0] + "…"

def h8(*parts):
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:12]

def iso(s):
    """ISO-ish timestamp -> epoch seconds, or None."""
    if not s: return None
    try:
        s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], str(s).strip().replace("Z", "+00:00"), count=1)
        s = re.sub(r"([+-]\d\d)(\d\d)$", r"\1:\2", s)
        d = datetime.fromisoformat(s)
        return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).timestamp()
    except Exception:
        return None

def local(s, utc_offset):
    """'11/10/26, 10:25 AM MT' or '26-11-10 12:45 PM' (local time) -> epoch, or None."""
    s = re.sub(r"\s+(?!AM$|PM$)[A-Z]{2,4}$", "", (s or "").strip())
    for f in ("%m/%d/%y, %I:%M %p", "%y-%m-%d %I:%M %p", "%m/%d/%Y %I:%M %p", "%b %d, %Y, %I:%M %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, f).replace(tzinfo=timezone(timedelta(hours=utc_offset))).timestamp()
        except ValueError:
            pass
    return None

# ------------------------------------------------------------------ what counts
AVALANCHE = re.compile(r"avalanche(?!\s+(gate|road|rd|creek|crescent|drive|lane|way|street)\b)|\bavy\b|avalanche control|snow ?slide", re.I)
CLOSURE = re.compile(r"(road|route|highway|hwy|notch|pass|gap|bridge|parkway|canyon|summit|freeway|interstate)\s+(is\s+|are\s+|will be\s+|has been\s+|remains\s+)?(now\s+)?clos"
                     r"|closed to (all |through |thru )?(traffic|vehicles)|full(y)? clos|closed for the (season|winter)|is closed from|closed between|closed in both"
                     r"|seasonal(ly)? clos|winter clos|road closed|all lanes closed|\bclosed\.|closure in effect|no access|\bfermeture\b|closed due to", re.I)
REOPEN = re.compile(r"\b(re-?opened|re-?opening|now open|open to all traffic|has reopened|reopens?)\b", re.I)
HAZARD = re.compile(r"rocks? on (the )?road|rock ?slide|rock ?fall|land ?slide|mud ?slide|debris flow|washout|washed out|flood|downed (trees?|power ?lines?)"
                    r"|jack-?knife|white-?out|blizzard|sinkhole|wild ?fire|multi-?vehicle|pile-?up|fatal", re.I)
CHAINS = re.compile(r"chains? (are |or snow tires are )?required|chain (control|law|restriction)s? (is |are )?in effect|traction (law|tires required)|R-?[123]\b chain", re.I)
WEATHER = re.compile(r"\bsnow|\bice\b|\bicy\b|\bwinter|blizzard|white-?out|\bdrift|avalanche|\w*slide\b|rock ?fall|\bflood|washout|\bstorm|visibility|\bweather|neige|verglas|hiver|glissement|inondation", re.I)
BIG_ROAD = re.compile(r"\b(I-?\s?\d+|US-?\s?\d+|Interstate|Trans-?Canada|HWY-?\s?\d{1,2}\b|Highway \d{1,2}\b|Route \d{1,3}\b)", re.I)
LANES = re.compile(r"\b(?!all )(\w+ )?(lanes?(\(s\))?|shoulders?|ramps?|connector)( is| are| has been| have been)? (closed|re-?opened)( to traffic)?", re.I)
PLANNED = re.compile(r"construction|road ?work|paving|maintenance|bridge (work|repair|replacement|rehab)|utility|guardrail|culvert|resurfac|parade|marathon|special event"
                     r"|festival|railway|railroad|travaux|chantier|tree (trimming|pruning|removal)|striping|signal (work|install)", re.I)
NOISE = re.compile(r"rest area|weigh station|\bon-?ramp|\boff-?ramp|\bramps?\b|sidewalk|pedestrian|wildlife advisory|brake check|scale closed|turn(ing)? lanes? closed"
                   r"|shoulder closed|port of entry|truck parking|oversize|width limit|length limit|bike (path|lane)", re.I)

def classify(text):
    if AVALANCHE.search(text): return "AVALANCHE"
    text = LANES.sub(" ", re.sub(r"all lanes (are )?closed", "road closed", text, flags=re.I))
    if CLOSURE.search(text): return "CLOSED"
    if REOPEN.search(text): return "REOPENED"
    if HAZARD.search(text): return "HAZARD"
    if CHAINS.search(text): return "CHAINS"
    return ""

# ski areas, passes and mountain towns: name,lat,lon
GAZ = """Mt Baker,48.86,-121.66;Stevens Pass,47.75,-121.09;Snoqualmie Pass,47.42,-121.41;Crystal Mountain,46.93,-121.47;White Pass,46.64,-121.39;Mission Ridge,47.29,-120.40
49 Degrees North,48.30,-117.56;Mt Spokane,47.92,-117.10;Loup Loup,48.39,-119.91;Bluewood,46.08,-117.85;Hurricane Ridge,47.97,-123.50;Washington Pass,48.52,-120.65;Chinook Pass,46.87,-121.52
Blewett Pass,47.33,-120.58;Mt Hood Meadows,45.33,-121.67;Government Camp,45.30,-121.76;Mt Bachelor,43.98,-121.69;Santiam Pass,44.41,-121.87;Willamette Pass,43.60,-122.04;Mt Ashland,42.08,-122.72
Anthony Lakes,44.96,-118.23;Siskiyou Summit,42.06,-122.60;Palisades Tahoe,39.20,-120.24;Northstar,39.27,-120.12;Donner Summit,39.31,-120.33;Heavenly,38.93,-119.94;Kirkwood,38.68,-120.07
Echo Summit,38.81,-120.03;Homewood,39.08,-120.16;Mt Rose,39.33,-119.89;Diamond Peak,39.25,-119.92;Bear Valley,38.49,-120.04;Dodge Ridge,38.19,-119.96;Mammoth,37.63,-119.03;June Mountain,37.77,-119.09
Tioga Pass,37.91,-119.26;Sonora Pass,38.33,-119.64;Ebbetts Pass,38.54,-119.81;Monitor Pass,38.67,-119.62;Carson Pass,38.69,-119.99;Luther Pass,38.79,-119.95;China Peak,37.24,-119.16
Mt Shasta,41.32,-122.20;Big Bear,34.23,-116.89;Snow Valley,34.22,-117.04;Mountain High,34.37,-117.69;Mt Baldy,34.27,-117.62;Lassen,40.47,-121.51;Lee Canyon,36.30,-115.68
Sun Valley,43.68,-114.37;Schweitzer,48.37,-116.62;Bogus Basin,43.76,-116.10;Brundage,45.00,-116.15;Tamarack,44.67,-116.12;Grand Targhee,43.79,-110.96;Silver Mountain,47.50,-116.11
Lookout Pass,47.46,-115.70;Pebble Creek,42.78,-112.10;Soldier Mountain,43.49,-114.83;Kelly Canyon,43.64,-111.63;Pomerelle,42.32,-113.61;Galena Summit,43.87,-114.71;Lolo Pass,46.64,-114.58
Lost Trail,45.69,-113.95;Teton Pass,43.50,-110.96;Big Sky,45.28,-111.40;Bridger Bowl,45.82,-110.90;Whitefish,48.48,-114.35;Red Lodge,45.19,-109.34;Discovery,46.25,-113.24
Montana Snowbowl,47.01,-113.99;Showdown,46.84,-110.70;Great Divide,46.75,-112.31;Maverick,45.43,-113.13;Beartooth Pass,44.97,-109.47;Marias Pass,48.32,-113.35;Logan Pass,48.70,-113.72
Cooke City,45.02,-109.93;West Yellowstone,44.66,-111.10;Jackson Hole,43.59,-110.83;Snow King,43.47,-110.76;Togwotee Pass,43.75,-110.07;Snowy Range,41.35,-106.19;Hogadon,42.75,-106.33
Sleeping Giant,44.49,-109.94;White Pine,42.98,-109.76;Hoback,43.29,-110.67;Little Cottonwood,40.58,-111.65;Big Cottonwood,40.61,-111.59;Park City,40.65,-111.51;Snowbasin,41.22,-111.86
Powder Mountain,41.38,-111.78;Sundance,40.39,-111.59;Brian Head,37.70,-112.85;Beaver Mountain,41.97,-111.54;Eagle Point,38.32,-112.38;Vail,39.64,-106.37;Beaver Creek,39.60,-106.52
Breckenridge,39.48,-106.07;Keystone,39.61,-105.94;Loveland Pass,39.66,-105.88;Copper Mountain,39.50,-106.15;Berthoud Pass,39.80,-105.78;Winter Park,39.89,-105.76;Steamboat,40.46,-106.80
Rabbit Ears Pass,40.38,-106.61;Aspen,39.19,-106.82;Snowmass,39.21,-106.95;Independence Pass,39.11,-106.56;Crested Butte,38.90,-106.97;Monarch Pass,38.51,-106.33;Telluride,37.94,-107.81
Red Mountain Pass,37.90,-107.71;Silverton,37.81,-107.66;Purgatory,37.63,-107.81;Wolf Creek Pass,37.47,-106.79;Eldora,39.94,-105.58;Tennessee Pass,39.36,-106.30;Sunlight,39.40,-107.34
Powderhorn,39.07,-108.15;Cameron Pass,40.52,-105.89;Vail Pass,39.53,-106.22;Hoosier Pass,39.36,-106.06;Fremont Pass,39.37,-106.19;Lizard Head Pass,37.81,-107.91;Kenosha Pass,39.41,-105.76
Georgetown,39.71,-105.70;Glenwood Canyon,39.57,-107.22;Trail Ridge,40.42,-105.75;Granby,40.09,-105.94;Taos Ski Valley,36.60,-105.45;Angel Fire,36.39,-105.29;Red River,36.71,-105.41
Ski Santa Fe,35.80,-105.80;Sipapu,36.15,-105.55;Ski Apache,33.40,-105.79;Pajarito,35.89,-106.39;Sandia Peak,35.21,-106.41;Arizona Snowbowl,35.33,-111.71;Sunrise Park,33.97,-109.56
Mt Lemmon,32.45,-110.78;Alyeska,60.96,-149.10;Turnagain Pass,60.78,-149.21;Hatcher Pass,61.77,-149.31;Thompson Pass,61.13,-145.73;Eaglecrest,58.27,-134.52;Arctic Valley,61.25,-149.53
Mount Bohemia,47.39,-88.01;Lutsen,47.66,-90.72;Spirit Mountain,46.72,-92.22;Boyne Mountain,45.16,-84.93;Boyne Highlands,45.47,-84.93;Crystal Mountain MI,44.52,-85.99;Granite Peak,44.93,-89.68
Big Powderhorn,46.50,-90.10;Marquette Mountain,46.51,-87.42;Terry Peak,44.33,-103.83;Giants Ridge,47.57,-92.30;Whiteface,44.37,-73.90;Gore Mountain,43.67,-74.01;Hunter,42.20,-74.23
Windham,42.30,-74.26;Belleayre,42.13,-74.51;Holiday Valley,42.26,-78.67;Camelback,41.05,-75.36;Blue Mountain PA,40.82,-75.52;Seven Springs,40.02,-79.30;Snowshoe,38.41,-79.99
Canaan Valley,39.04,-79.40;Wisp,39.56,-79.36;Sugar Mountain,36.13,-81.86;Beech Mountain,36.19,-81.88;Cataloochee,35.56,-83.09;Jiminy Peak,42.56,-73.29;Wachusett,42.50,-71.89
Jay Peak,44.93,-72.50;Burke,44.57,-71.92;Smugglers' Notch,44.56,-72.79;Stowe,44.53,-72.78;Bolton Valley,44.42,-72.85;Mad River Glen,44.20,-72.92;Sugarbush,44.14,-72.90
Middlebury Snow Bowl,43.94,-72.95;Killington,43.62,-72.80;Okemo,43.40,-72.72;Bromley,43.23,-72.94;Magic Mountain,43.20,-72.77;Stratton,43.11,-72.91;Mount Snow,42.96,-72.92
Cannon,44.16,-71.70;Loon,44.04,-71.62;Waterville Valley,43.95,-71.52;Bretton Woods,44.26,-71.44;Attitash,44.08,-71.23;Cranmore,44.06,-71.11;Pinkham Notch,44.26,-71.24
Gunstock,43.54,-71.36;Ragged,43.48,-71.84;Mount Sunapee,43.33,-72.08;Sugarloaf,45.03,-70.31;Saddleback,44.95,-70.51;Sunday River,44.47,-70.86;Big Moose,45.50,-69.68
Whistler,50.11,-122.95;Cypress,49.40,-123.20;Grouse,49.38,-123.08;Seymour,49.37,-122.95;Sasquatch,49.38,-121.94;Allison Pass,49.06,-120.78;Coquihalla Summit,49.60,-121.05
Sun Peaks,50.88,-119.89;Big White,49.72,-118.93;Silver Star,50.36,-119.06;Apex,49.39,-119.90;Revelstoke,50.96,-118.16;Rogers Pass,51.30,-117.52;Kicking Horse,51.30,-117.05
Panorama,50.46,-116.24;Kimberley,49.68,-116.00;Fernie,49.46,-115.09;Red Mountain,49.10,-117.82;Whitewater,49.44,-117.15;Kootenay Pass,49.06,-117.04;Mount Washington BC,49.74,-125.30
Hudson Bay Mountain,54.77,-127.28;Shames,54.49,-128.96;Powder King,55.35,-122.63;Duffey Lake,50.38,-122.45;Squamish,49.70,-123.15;Three Valley Gap,50.93,-118.47;Lake Louise,51.44,-116.16
Sunshine Village,51.11,-115.76;Banff,51.20,-115.60;Kananaskis,50.94,-115.15;Marmot Basin,52.80,-118.08;Castle Mountain,49.32,-114.41;Icefields Parkway,52.22,-117.22;Highwood Pass,50.60,-114.98
Canmore,51.09,-115.36;Field,51.40,-116.49;Crowsnest Pass,49.63,-114.69;Waterton,49.05,-113.91;Tremblant,46.21,-74.58;Mont-Sainte-Anne,47.07,-70.90;Le Massif,47.28,-70.57
Stoneham,47.03,-71.38;Sutton,45.10,-72.56;Bromont,45.29,-72.63;Orford,45.31,-72.24;Owl's Head,45.06,-72.30;Mont Edouard,48.14,-70.27;Chic-Chocs,48.95,-66.10;Saint-Sauveur,45.88,-74.16
Blue Mountain ON,44.50,-80.31;Marble Mountain,48.93,-57.83;Crabbe Mountain,46.12,-67.10;Asessippi,50.96,-101.38;Mount Sima,60.61,-135.07;White Pass YT,59.62,-135.14"""
GAZ = [(a, float(b), float(c)) for a, b, c in (x.split(",") for x in re.split(r"[;\n]", GAZ) if x.strip())]
PLACES = re.compile(r"\b(" + "|".join(sorted({re.escape(re.sub(r" (MI|PA|BC|ON|YT)$", "", n)) for n, _, _ in GAZ if len(n) > 5}, key=len, reverse=True)) +
                    r"|Donner|Tahoe|Truckee|Sierra|Yosemite|Yellowstone|Grand Teton|Glacier National|Mount Rainier|Mt\.? Rainier|Paradise|Mount Hood|Mt\.? Hood|North Cascades"
                    r"|Cottonwood Canyon|Parleys|Provo Canyon|Ogden Canyon|Logan Canyon|Eisenhower|Johnson Tunnel|Floyd Hill|Gunnison|Durango|Ouray|Leadville|Frisco|Silverthorne"
                    r"|Ketchum|Stanley|McCall|Driggs|Victor|Wilson|Alpine Junction|Bozeman|Gallatin Canyon|Girdwood|Seward Highway|Valdez|Haines|Juneau|Sea[- ]to[- ]Sky|Coquihalla"
                    r"|Pemberton|Golden|Nelson|Rossland|Jasper|Kootenay|Yoho|Charlevoix|Laurentides|Gasp[ée]sie|Franconia|Crawford Notch|Kancamagus|Appalachian Gap|Lincoln Gap"
                    r"|Adirondack|Catskill|Pocono|Blue Ridge|ski (area|resort|hill)|trailhead|sno-?park)\b", re.I)

def near(lat, lon, km=30):
    try: lat, lon = float(lat), float(lon)
    except (TypeError, ValueError): return None
    p = math.pi / 180; best = None
    for n, a, b in GAZ:
        d = 6371 * math.hypot((lon - b) * p * math.cos((lat + a) / 2 * p), (lat - a) * p)
        if d <= km and (best is None or d < best[1]): best = (n, d)
    return best

def judge(road, text, lat=None, lon=None, full=False, routes=None, planned=None, km=30):
    """Return (label, why) if this road event is worth an item, else None."""
    blob = f"{road} {text}"
    kind = classify(blob)
    if kind == "AVALANCHE": return "AVALANCHE", "avalanche"
    if NOISE.search(blob): return None
    nb = near(lat, lon, km)
    place = PLACES.search(blob)
    route = routes.search(road or "") if routes else None
    geo = nb or place or route
    planned = PLANNED.search(blob) if planned is None else planned
    full = full or kind == "CLOSED"
    why = (f"near {nb[0]}" if nb else place.group(0) if place else "ski route" if route else "weather")
    if full and not planned and (geo or (WEATHER.search(blob) and BIG_ROAD.search(road or blob[:60]))): return "CLOSED", why
    if full and planned and (nb or place): return "CLOSED", why
    if kind == "REOPENED" and (nb or place or route) and not planned: return "REOPENED", why
    if kind == "HAZARD" and geo and not planned: return "HAZARD", why
    return None

def item(key, title, link, body, t=None, kind=""):
    return {"key": str(key), "title": re.sub(r"\s+", " ", title).strip()[:300], "link": link, "body": body.strip(), "t": t, "kind": kind}

# ------------------------------------------------------------------ adapters: roads
IBI_Q = urllib.parse.quote(json.dumps({"columns": [{"data": None, "name": ""}], "order": [], "start": 0, "length": 500, "search": {"value": ""}}))

def ibi(s):
    """Sites on the Arcadis/IBI '511' platform: the public list pages' data endpoint needs no key."""
    out, ok = [], 0
    for layer in s.get("layers", ["Closures", "Incidents"]):
        try:
            j = fetch_json(f"https://{s['host']}/List/GetData/{layer}?query={IBI_Q}&lang=en")
        except Exception:
            continue
        ok += 1
        for r in j.get("data", []):
            if s.get("state") and r.get("state") and r.get("state") != s["state"]: continue   # neighbours' events shown on the same map
            road = r.get("roadwayName") or ""
            text = strip(r.get("description") or "") + " " + (r.get("comment") or "")
            full = bool(r.get("isFullClosure")) or bool(re.search(r"all lanes closed|road closed", text, re.I))
            planned = bool(PLANNED.search((r.get("eventSubType") or "") + " " + text))
            v = judge(road, text, full=full, routes=s.get("routes"), planned=planned)
            if not v: continue
            t = local(r.get("lastUpdated"), s.get("utc", -7))
            out.append(item(f"{r.get('id')}|{v[0]}", f"[{s['st']}] {road}: {v[0]} — {short(r.get('comment') or text)}", f"https://{s['host']}/",
                            f"{text.strip()}\n\nWhy flagged: {v[1]}. Layer: {layer}. Last updated (local): {r.get('lastUpdated')}. "
                            f"Source: {s['name']} ({s['host']}, event {r.get('id')}).", t, v[0]))
    if not ok: raise RuntimeError("no layer answered")
    return out

def cars(s):
    """Castle Rock CARS 511 sites (COtrip, 511mn, mass511, 511ny): public event feed, no key."""
    out = []
    for e in fetch_json(f"https://{s['host']}/events_v1/api/eventReports"):
        d = e.get("eventDescription") or {}
        head = d.get("headlinePhrase") or ""
        full_html = d.get("descriptionFull") or ""
        text = f"{d.get('descriptionHeader') or ''} {strip(full_html)}"
        p = (e.get("location") or {}).get("primaryPoint") or {}
        road = (e.get("location") or {}).get("routeDesignator") or ""
        full = head == "Road Closed" or bool(re.search(r"road closed|closed to all|closed in both", text, re.I))
        if "winterDriving" in full_html and not full and not AVALANCHE.search(text): continue
        v = judge(road, text, p.get("lat"), p.get("lon"), full=full, planned=bool(PLANNED.search(head + " " + text) or "intermittent" in text.lower()), km=s.get("km", 35))
        if not v: continue
        t = ((e.get("updateTime") or {}).get("time") or 0) / 1000 or None
        out.append(item(f"{e.get('id')}|{v[0]}", f"[{s['st']}] {road}: {v[0]} — {short(text)}", s["site"],
                        f"{text.strip()}\n\nWhy flagged: {v[1]}. Headline: {head}. Priority {e.get('priority')}. "
                        f"Source: {s['name']} (event {e.get('id')}).", t, v[0]))
    return out

def first_xy(g):
    c = (g or {}).get("coordinates")
    while isinstance(c, list) and c and isinstance(c[0], list): c = c[0]
    return (c[1], c[0]) if isinstance(c, list) and len(c) >= 2 else (None, None)

def cars511x(s):
    """Castle Rock's newer '511x' sites (511NY since 2026): the map's own event feed, no key."""
    out = []
    for f in fetch_json(s["base"] + "/events/map-features").get("features", []):
        p = f.get("properties") or {}
        desc = p.get("description") or ""
        text = f"{p.get('title') or p.get('tooltip') or ''} {strip(desc)}"
        full = bool(re.search(r"road closed|closed to all|all lanes (are )?closed|closed in both|bridge closed", text, re.I)) or "closure" in (p.get("icon") or "")
        if "winterDriving" in desc and not full and not AVALANCHE.search(text): continue
        b = f.get("bounds") or {}
        lat, lon = (b.get("minLat"), b.get("minLon")) if isinstance(b, dict) and b.get("minLat") is not None else first_xy(f.get("geometry"))
        v = judge(p.get("route") or "", text, lat, lon, full=full, planned=bool(PLANNED.search(text) or "intermittent" in text.lower()), km=s.get("km", 20))
        if not v: continue
        out.append(item(f"{p.get('id')}|{v[0]}", f"[{s['st']}] {p.get('route')}: {v[0]} — {short(text)}", s["site"],
                        f"{text.strip()}\n\nWhy flagged: {v[1]}. Priority {p.get('priority')}. Source: {s['name']} (event {p.get('id')}).",
                        (p.get("updated") or 0) / 1000 or None, v[0]))
    return out

BC_ROADS = {"Highway 1", "Highway 3", "Highway 3A", "Highway 3B", "Highway 4", "Highway 5", "Highway 5A", "Highway 6", "Highway 16", "Highway 19",
            "Highway 23", "Highway 28", "Highway 31", "Highway 31A", "Highway 33", "Highway 93", "Highway 95", "Highway 95A", "Highway 97", "Highway 97A", "Highway 97C", "Highway 99"}

def bc(s):
    events, off = [], 0
    while True:
        b = fetch_json(f"https://api.open511.gov.bc.ca/events?format=json&status=ACTIVE&limit=500&offset={off}").get("events", [])
        events += b
        if len(b) < 500 or off > 5000: break
        off += 500
    out = []
    for e in events:
        roads = e.get("roads") or []
        names = [r.get("name", "") for r in roads]
        desc = e.get("description", "") or ""
        blob = " ".join(r.get("from", "") or "" for r in roads) + " " + desc
        closed = any((r.get("state") or "") == "CLOSED" for r in roads)
        sev, et = e.get("severity", ""), e.get("event_type", "")
        areas = " ".join(a.get("name", "") for a in (e.get("areas") or []))
        place = PLACES.search(blob)
        if not (place or any(n in BC_ROADS for n in names)) or (NOISE.search(desc) and not AVALANCHE.search(desc)): continue
        kind = classify(desc) or ("CLOSED" if closed else "")
        lowland = bool(re.search(r"Lower Mainland|Vancouver Island", areas))
        if not kind and (sev != "MAJOR" or re.search(r"\bcleared\b", desc, re.I) or (lowland and not place)): continue
        if re.match(r"\s*special event", desc, re.I) and kind != "AVALANCHE": continue
        if et == "SPECIAL_EVENT" and kind != "AVALANCHE": continue
        planned = et in ("CONSTRUCTION", "SPECIAL_EVENT")
        lowland = bool(re.search(r"Lower Mainland|Vancouver Island", areas))
        if planned and kind != "AVALANCHE" and not (place or (sev == "MAJOR" and closed and not lowland)): continue
        if kind in ("HAZARD", "REOPENED", "CHAINS") and not (place or sev == "MAJOR"): continue
        road = next((n for n in names if n != "Other Roads"), None) or (roads[0].get("from") if roads else "Road")
        label = kind or "MAJOR " + et.replace("_", " ").title()
        out.append(item(f"{e.get('id')}|{kind}|{sev}|{closed}", f"[BC] {road}: {label} — {short(desc)}", "https://www.drivebc.ca/",
                        f"{desc}\n\nType: {et}. Severity: {sev}. Area: {areas}. Source: DriveBC (Open511 event {e.get('id')}).",
                        iso(e.get("updated") or e.get("created")), kind or "MAJOR"))
    return out

NE_ROUTES = {("VT", r) for r in ("VT108", "VT100", "VT17", "VT242", "VT11", "VT30", "VT9", "VT103", "VT125", "VT73", "US4", "LINCOLNGAPRD")} | \
            {("NH", r) for r in ("NH112", "NH16", "US302", "NH49", "NH18", "US2", "PRESIDENTIALHWY", "KANCAMAGUSHWY")} | \
            {("ME", r) for r in ("ME27", "ME4", "ME26", "ME17", "ME142", "ME16")}

def newengland(s):
    ns, out = "{http://its.gov/c2c_icd}", []
    for net, st in (("Vermont", "VT"), ("NewHampshire", "NH"), ("Maine", "ME")):
        for dtype, tag in (("incidentData", "incident"), ("laneClosureData", "laneClosure")):
            root = ET.fromstring(fetch(f"https://nec-por.ne-compass.com/NEC.XmlDataPortal/api/c2c?networks={net}&dataTypes={dtype}"))
            for e in root.iter(ns + tag):
                g = lambda p: (e.findtext("/".join(ns + x for x in p.split("/"))) or "").strip()
                desc, roadway, city = g("desc"), g("startLocation/roadway"), g("startLocation/city")
                try: lat, lon = int(g("startLocation/lat")) / 1e6, int(g("startLocation/lon")) / 1e6
                except ValueError: lat = lon = None
                nb = near(lat, lon, 12)
                r = re.sub(r"[^A-Z0-9]", "", roadway.upper())
                m = re.match(r"^(?:ME|RT|ROUTE|SR)(\d+[A-Z]?)", r) if st == "ME" else None
                onroute = (st, "ME" + m.group(1) if m else r) in NE_ROUTES
                if not (nb or onroute) or NOISE.search(desc): continue
                lanes, etype, sev = g("affectedLanesDescription"), g("eventType"), g("severity")
                kind = classify(desc + " " + lanes)
                if lanes.lower() == "all lanes closed": kind = kind or "CLOSED"
                if tag == "laneClosure" and not (kind or etype == "PSA"): continue
                if PLANNED.search(desc + " " + etype) and not onroute and not (nb and nb[1] <= 6) and kind != "AVALANCHE": continue
                if tag == "incident" and not (kind or sev == "High" or nb): continue
                label = kind or ("ALERT" if etype == "PSA" else "INCIDENT")
                upd = g("lastUpdatedTimestamp") or g("createdTimestamp")
                out.append(item(f"{e.get('id')}|{kind}|{sev}|{etype}", f"[{st}] {roadway}{', ' + city if city else ''}: {label} — {short(desc)}",
                                "https://newengland511.org/", f"{desc}\n\nLanes: {lanes or 'n/a'}. Type: {etype}. Severity: {sev}. "
                                f"Nearest ski area: {nb[0] + ' (%.0f km)' % nb[1] if nb else 'n/a'}. Source: New England 511 / {st} DOT ({e.get('id')}).", iso(upd), label))
    return out

def caltrans_roads(s):
    """Caltrans 'Check Current Highway Conditions' text pages, one per route. A new or changed statement is a new item."""
    out = []
    for n in s["routes"]:
        try: page = fetch(f"https://roads.dot.ca.gov/roadscell.php?roadnumber={n}").decode("utf-8", "replace")
        except Exception: continue
        i = page.find("latest reported as of")
        seg = page[i:] if i > 0 else page
        hdr = re.search(r"<h3[^>]*>(.*?)</h3>", seg, re.S)
        road = strip(hdr.group(1)) if hdr else f"SR {n}"
        for am in re.finditer(r"<strong>\s*\[(.*?)\]\s*</strong>(.*?)(?=<strong>\s*\[|</div>|$)", seg, re.S):
            area = strip(am.group(1))
            for st in re.split(r"(?:<br\s*/?>\s*){2,}|</p>\s*<p>", am.group(2)):
                t = strip(st)
                if len(t) < 12 or re.search(r"No traffic restrictions|research chain control|road information call|brake check", t, re.I): continue
                kind = classify(t)
                if kind == "CLOSED" and (PLANNED.search(t) or re.search(r"\d{4} hrs|each night|connector|ramp", t, re.I)) and not re.search(r"snow|winter|slide|avalanche|flood|rock", t, re.I): continue
                if kind not in ("AVALANCHE", "CLOSED", "CHAINS", "HAZARD", "REOPENED"): continue
                out.append(item(f"{n}|{h8(area, t)}", f"[CA] {road}: {kind} — {short(t)}", f"https://roads.dot.ca.gov/roadscell.php?roadnumber={n}",
                                f"{t}\n\nArea: {area}. Source: Caltrans highway conditions, {road}.", None, kind))
    if not out and not s.get("_any"):
        pass
    return out

def tripcheck(s):
    out, seen = [], set()
    for f in ("INCD.js", "EVENT.js"):
        for ft in fetch_json(f"https://www.tripcheck.com/Scripts/map/data/{f}").get("features", []):
            a = ft.get("attributes") or {}
            if a.get("incidentId") in seen: continue
            seen.add(a.get("incidentId"))
            text = f"{a.get('eventTypeName') or ''}: {a.get('eventSubTypeName') or ''}. {a.get('comments') or ''} ({a.get('beginMarker') or a.get('locationName') or ''}). Impact: {a.get('odotSeverityDescript') or ''}"
            full = bool(re.search(r"clos", a.get("odotSeverityDescript") or "", re.I)) and not re.search(r"lane|shoulder", a.get("odotSeverityDescript") or "", re.I)
            v = judge(a.get("route") or "", text, a.get("startLatitude"), a.get("startLongitude"), full=full, km=35)
            if not v: continue
            out.append(item(f"{a.get('incidentId')}|{v[0]}", f"[OR] {a.get('route')}: {v[0]} — {short(a.get('comments') or text)}", "https://www.tripcheck.com/",
                            f"{text}\n\nWhy flagged: {v[1]}. Source: ODOT TripCheck (incident {a.get('incidentId')}).", iso(a.get("lastUpdated")), v[0]))
    for ft in fetch_json("https://www.tripcheck.com/Scripts/map/data/RW.js").get("features", []):
        a = ft.get("attributes") or {}
        d = a.get("chainRestrictionDesc") or ""
        if not re.search(r"required|closed|prohibited", d, re.I) or re.search(r"^carry", d, re.I): continue
        out.append(item(f"rw{a.get('id')}|{h8(d)}", f"[OR] {a.get('locationName')} ({a.get('linkName')}): CHAINS — {d}", "https://www.tripcheck.com/",
                        f"{a.get('locationName')}: {d}. Pavement: {a.get('pavementConditionDesc')}. Weather: {a.get('weatherConditionDesc')}. "
                        f"New snow: {a.get('snowfall')} in. Source: ODOT TripCheck road-weather report.", iso(a.get("entryTime")), "CHAINS"))
    return out

WY_ROUTES = re.compile(r"WY ?22\b|WY ?390|US ?26|US ?89|US ?191|US ?189|US ?287|WY ?130|WY ?230|US ?212|WY ?296|US ?14|US ?16\b|WY ?28\b|WY ?70\b|US ?20\b|WY ?120", re.I)

def wydot(s):
    page = fetch("https://www.wyoroad.info/highway/conditions/RoadClosures.html").decode("utf-8", "replace")
    out, route = [], ""
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        tds = re.findall(r"<td([^>]*)>(.*?)</td>", tr, re.S)
        idx = next((i for i, (a, _) in enumerate(tds) if "rpttime" in a), None)
        if idx is None or idx < 4: continue
        first = strip(tds[0][1])
        if idx >= 5 and re.match(r"^(I|US|WY) ?\d+", first): route = first
        loc, reason, adv, restr = (strip(tds[idx + k][1]) for k in (-4, -3, -2, -1))
        if not re.search(r"clos", reason, re.I) or re.search(r"closed to light", reason, re.I): continue
        if not (WY_ROUTES.search(route) or AVALANCHE.search(reason + adv) or PLACES.search(loc)): continue
        kind = "AVALANCHE" if AVALANCHE.search(reason + adv) else "CLOSED"
        out.append(item(f"{route}|{loc}|{h8(reason)}", f"[WY] {route}: {kind} — {loc}: {reason}", "https://www.wyoroad.info/highway/conditions/RoadClosures.html",
                        f"{route}, {loc}. {reason}. Advisories: {adv or 'none'}. Restrictions: {restr or 'none'}. Reported {strip(tds[idx][1])} (Mountain). "
                        f"Source: WYDOT closures and advisories.", local(strip(tds[idx][1]), -7), kind))
    return out

def mdt(s):
    out = []
    for f in fetch_json("https://mt.cdn.iteris-atis.com/geojson/icons/metadata/icons.events.geojson").get("features", []):
        p = f.get("properties") or {}
        c = (f.get("geometry") or {}).get("coordinates") or [None, None]
        text = strip(p.get("enhanced_report") or p.get("report") or "")
        v = judge(p.get("route") or "", text, c[1], c[0], km=40)
        if not v: continue
        out.append(item(f"{p.get('event_id')}|{v[0]}", f"[MT] {p.get('route')}: {v[0]} — {short(text)}", "https://www.511mt.net/",
                        f"{text}\n\nWhy flagged: {v[1]}. Source: Montana DOT traveler information (event {p.get('event_id')}).", p.get("last_update"), v[0]))
    return out

QC_REG = re.compile(r"Laurentides|Lanaudi|Capitale|Charlevoix|Estrie|Gasp|Saguenay|Bas-Saint|Chaudi|Outaouais|Mauricie|C[ôo]te-Nord", re.I)

def quebec(s):
    j = fetch_json("https://ws.mapserver.transports.gouv.qc.ca/swtq?service=wfs&version=2.0.0&request=getfeature&typename=ms:evenements"
                   "&outfile=AvertissementRoutier&srsname=EPSG:4326&outputformat=geojson")
    out = []
    for f in j.get("features", []):
        p = f.get("properties") or {}
        g = f.get("geometry") or {}
        lat = lon = None
        if g.get("type") == "Point": lon, lat = g["coordinates"][:2]
        ent = p.get("entrave") or ""
        text = f"{ent}. {p.get('localisation') or ''}. Cause: {p.get('cause') or ''}. {p.get('consequence') or ''} {p.get('detour') or ''}"
        full = ent.lower().startswith("fermeture")
        if not full and not AVALANCHE.search(text): continue
        nb = near(lat, lon, 30)
        if not (nb or AVALANCHE.search(text) or (QC_REG.search(str(p.get("regions") or "")) and WEATHER.search(p.get("cause") or ""))): continue
        if PLANNED.search(p.get("cause") or "") and not nb: continue
        kind = "AVALANCHE" if AVALANCHE.search(text) else "CLOSED"
        out.append(item(f"{p.get('identifiant')}|{kind}", f"[QC] Route {p.get('numeroRoute')}, {p.get('municipalite')}: {kind} — {short(text)}",
                        "https://www.quebec511.info/en/Diffusion/EtatReseau/Avertissements.aspx",
                        f"{text}\n\nRegion: {p.get('regions')}. In effect since {p.get('enVigueurDepuis')}. Source: Québec 511 (MTMD open data, event {p.get('identifiant')}).",
                        iso((p.get("enVigueurDepuis") or "") + "-05:00") if p.get("enVigueurDepuis") else None, kind))
    return out

def arcgis(s):
    """Road-status layers where each row is a road segment: an item appears when a segment's status changes."""
    out = []
    for f in fetch_json(s["url"]).get("features", []):
        a = f.get("attributes") or {}
        name, code = a.get(s["name_f"]), a.get(s["status_f"])
        status = s["codes"].get(code, str(code))
        if not name or status.startswith("Open"): continue
        if s.get("route_f") and a.get(s["route_f"]) and str(a[s["route_f"]]) not in name: name = f"SR {a[s['route_f']]} {name}"
        note = " ".join(str(a.get(k) or "") for k in s.get("note_f", []))
        kind = "CLOSED" if "Closed" in status else "CHAINS"
        out.append(item(f"{name}|{status}", f"[{s['st']}] {name}: {kind} — {status}. {short(note)}", s["site"],
                        f"{name}: {status}. {note}\n\nSource: {s['name']}.", (a.get(s.get("time_f", "")) or 0) / 1000 or None, kind))
    return out

def nps(s):
    out, key = [], os.environ.get("NPS_API_KEY") or "DEMO_KEY"
    for park in s["parks"]:
        try: j = fetch_json(f"https://developer.nps.gov/api/v1/alerts?parkCode={park}&api_key={key}")
        except Exception: continue
        for a in j.get("data", []):
            text = f"{a.get('title') or ''}. {a.get('description') or ''}"
            if not re.search(r"road|highway|avalanche|pass\b|closed|closure|snow", text, re.I): continue
            if a.get("category") not in ("Park Closure", "Danger", "Caution") and not AVALANCHE.search(text): continue
            kind = classify(text) or "ALERT"
            out.append(item(a.get("id"), f"[NPS {park.upper()}] {kind} — {a.get('title')}", a.get("url") or f"https://www.nps.gov/{park}/planyourvisit/conditions.htm",
                            f"{a.get('description')}\n\nCategory: {a.get('category')}. Source: National Park Service alerts ({park.upper()}).",
                            iso(str(a.get("lastIndexedDate") or "").replace(" ", "T")[:19]), kind))
    return out

# ------------------------------------------------------------------ adapters: avalanche
def inv_text(v):
    v = v or {}
    parts = [f"{v[k]} {k}" for k in ("caught", "buried", "injured", "killed") if v.get(k)]
    return ", ".join(parts) or (f"{v.get('involved')} involved" if v.get("involved") else "involvement not stated")

def caic_accidents(s):
    """National avalanche accident database (hosted by CAIC): every US avalanche accident report."""
    out = []
    for r in fetch_json("https://api.avalanche.state.co.us/api/v2/accident_reports?per=25&page=1"):
        t = iso(r.get("observed_at"))
        if not t or NOW() - t > 45 * 86400: continue
        d = r.get("public_report_detail") or {}
        v = r.get("involvement_summary") or {}
        label = "AVALANCHE FATALITY" if v.get("killed") else "AVALANCHE ACCIDENT"
        loc = d.get("location") or r.get("area") or r.get("landmark") or "location not stated"
        out.append(item(f"{r.get('id')}|{r.get('investigation_status')}|{v.get('killed')}",
                        f"[{label}] {r.get('state')}: {loc} — {inv_text(v)} ({(r.get('observed_at') or '')[:10]})",
                        f"https://avalanche.state.co.us/report/{r.get('id')}",
                        f"{d.get('involvement_summary') or inv_text(v)}. {strip(d.get('accident_summary') or r.get('description') or '')[:1200]}\n\n"
                        f"Date: {(r.get('observed_at') or '')[:10]}. State: {r.get('state')}. Closest avalanche center: {d.get('closest_avalanche_center')}. "
                        f"Investigation: {r.get('investigation_status')}. Activity: {json.dumps(v.get('normalized_travel_activity') or {})}. "
                        f"Source: national avalanche accident database (CAIC). Data: {r.get('url')}", iso(r.get("created_at")) or t, label))
    return out

def caic_caught(s):
    out = []
    u = "https://api.avalanche.state.co.us/api/v2/observation_reports?per=25&page=1&r[caught_in_avalanche_eq]=true&r[sorts][]=observed_at+desc"
    for r in fetch_json(u.replace("[", "%5B").replace("]", "%5D")):
        t = iso(r.get("observed_at"))
        if not t or NOW() - t > 21 * 86400: continue
        zone = (r.get("backcountry_zone") or {}).get("title") or ""
        desc = strip(r.get("description") or "")
        out.append(item(r.get("id"), f"[AVALANCHE INVOLVEMENT] {r.get('state') or 'CO'}: person caught — {short(r.get('area') or zone, 90)} ({(r.get('observed_at') or '')[:10]})",
                        f"https://avalanche.state.co.us/report/{r.get('id')}",
                        f"{desc[:1200]}\n\nArea: {r.get('area')}. Zone: {zone}. Involvement: {inv_text(r.get('involvement_summary'))}. "
                        f"Source: CAIC field report (caught in avalanche). Data: {r.get('url')}", iso(r.get("created_at")) or t, "AVALANCHE INVOLVEMENT"))
    return out

def nac_map(s):
    """avalanche.org national map: every US forecast zone. Items for HIGH/EXTREME danger and for avalanche warnings."""
    out = []
    for f in fetch_json("https://api.avalanche.org/v2/public/products/map-layer").get("features", []):
        p = f.get("properties") or {}
        lvl = p.get("danger_level") or -1
        w = (p.get("warning") or {}).get("product")
        zone = f"{p.get('center_id')} {p.get('name')} ({p.get('state')})"
        if w:
            out.append(item(f"{f.get('id')}|warning|{p.get('start_date')}", f"[AVALANCHE WARNING] {zone}", p.get("link") or p.get("center_link") or "https://avalanche.org/",
                            f"Avalanche {w} in effect for {p.get('name')}, {p.get('center')}. {json.dumps(p.get('warning'))[:600]}\n\nSource: avalanche.org national map.",
                            iso(p.get("start_date")), "AVALANCHE WARNING"))
        if lvl >= 4:
            out.append(item(f"{f.get('id')}|danger{lvl}", f"[AVALANCHE DANGER {'EXTREME' if lvl >= 5 else 'HIGH'}] {zone}", p.get("link") or "https://avalanche.org/",
                            f"Danger rating {p.get('danger')} ({lvl}/5) for {p.get('name')}, {p.get('center')}. {p.get('travel_advice') or ''}\n\nSource: avalanche.org national map.",
                            iso(p.get("start_date")), "AVALANCHE DANGER"))
    return out

NAC_CENTERS = "NWAC UAC GNFAC IPAC BTAC CNFAIC FAC SNFAC WAC CAIC EWYAIX HAC VAC WCMAC COAA CAAC EARAC PAC BAC CAC NYSAC ESAC HPAC KPAC MSAC MWAC SAC SOAIX TAC".split()
NAC_Q = ("query($center:String!,$startDate:String!,$endDate:String!){getObservationList(centerId:$center,startDate:$startDate,endDate:$endDate)"
         "{id observerType name startDate locationName locationPoint{lat lng} instability observationSummary}}")
INCIDENT = re.compile(r"\b(caught and|carried|swept)\b|\b(partial|full|complete)(ly)? (burial|buried)|\b(was|were|got|been|being) (\w+ )?(caught|buried)\b|\binjur\w+|\bfatal\w*"
                      r"|\bkilled\b|\bdeceased\b|\bairbag\b|\brescue[ds]?\b|\bclose call\b|\bnear miss\b", re.I)

PERSON = re.compile(r"\b(skier|rider|snowboarder|sledder|snowmobil\w+|climber|partner|person|member|friend|dog|i|we|he|she|they)\b[^.]{0,70}\b(caught|carried|swept|buried|injured)\b", re.I)
STRONG_INC = re.compile(r"\bfatal\w*|\bkilled\b|\bdeceased\b|\bairbag (deploy|pull)\w*|\b(partial|full|complete)(ly)? (burial|buried)|\brescued\b|\bclose call\b|\bnear miss\b", re.I)

def nac_obs(s):
    """Public observations on every US avalanche center's site (avalanche.org platform): items when someone was caught."""
    out, start, end, ok = [], time.strftime("%Y-%m-%d", time.gmtime(NOW() - 4 * 86400)), time.strftime("%Y-%m-%d", time.gmtime(NOW() + 86400)), 0
    for c in NAC_CENTERS:
        try:
            j = json.loads(fetch("https://api.avalanche.org/obs/v1/public/graphql", headers={"Content-Type": "application/json"},
                                 data=json.dumps({"query": NAC_Q, "variables": {"center": c, "startDate": start, "endDate": end}}).encode()))
        except Exception:
            continue
        ok += 1
        for o in ((j.get("data") or {}).get("getObservationList") or []):
            summ = strip(o.get("observationSummary") or "")
            caught = bool((o.get("instability") or {}).get("avalanches_caught"))
            m = PERSON.search(summ) or STRONG_INC.search(summ)
            if not caught and not (m and re.search(r"avalanche|slab|slide|slid\b", summ, re.I)): continue
            out.append(item(o.get("id"), f"[AVALANCHE {'INVOLVEMENT' if caught else 'REPORT'}] {c}: {o.get('locationName')} ({o.get('startDate')}) — {short(summ, 140)}",
                            f"https://avalanche.org/avalanche-centers/#{c}", f"{summ[:1500]}\n\nCenter: {c}. Location: {o.get('locationName')}. Observer: {o.get('observerType')}. "
                            f"Flag: {'observer ticked “caught in avalanche”' if caught else 'mentions ' + m.group(0)}. Find it on the {c} observations page (id {o.get('id')}). "
                            f"Source: avalanche.org observations.", iso(o.get("startDate")), "AVALANCHE INVOLVEMENT" if caught else "AVALANCHE REPORT"))
    if not ok: raise RuntimeError("no center answered")
    return out

def avcan(s):
    out = []
    for x in fetch_json("https://api.avalanche.ca/forecasts/en/products"):
        r = x.get("report") or {}
        owner = (x.get("owner") or {}).get("display") or (x.get("owner") or {}).get("value")
        title = r.get("title") or (x.get("area") or {}).get("name") or ""
        vals = [(v.get("rating") or {}).get("value") for d in (r.get("dangerRatings") or [])[:1] for v in (d.get("ratings") or {}).values()]
        top = "extreme" if "extreme" in vals else "high" if "high" in vals else None
        if top:
            out.append(item(f"{(x.get('area') or {}).get('id')}|{top}|{(r.get('dateIssued') or '')[:10]}", f"[AVALANCHE DANGER {top.upper()}] {owner}: {short(title, 120)}",
                            x.get("url") or "https://avalanche.ca/", f"{strip(r.get('highlights') or '')[:1200]}\n\nIssued {r.get('dateIssued')}. Source: Avalanche Canada forecasts.",
                            iso(r.get("dateIssued")), "AVALANCHE DANGER"))
        for n in x.get("notifications") or []:
            c = strip(n.get("content") or "")
            if n.get("type") == "info" and not re.search(r"warning|special", c, re.I): continue
            out.append(item(f"{(x.get('area') or {}).get('id')}|n|{h8(c)}", f"[AVALANCHE NOTICE] {owner}: {short(c, 150)}", x.get("url") or "https://avalanche.ca/",
                            f"{c}\n\nRegion: {title}. Source: Avalanche Canada forecasts.", iso(r.get("dateIssued")), "AVALANCHE WARNING"))
    try:
        sp = fetch_json("https://api.avalanche.ca/spaw/en/products")
        for x in (sp if isinstance(sp, list) else [sp] if sp else []):
            c = strip(json.dumps(x))[:1200]
            out.append(item(f"spaw|{h8(c)}", "[AVALANCHE WARNING] Avalanche Canada Special Public Avalanche Warning", "https://avalanche.ca/spaw",
                            f"{c}\n\nSource: Avalanche Canada.", None, "AVALANCHE WARNING"))
    except Exception:
        pass
    a, b = time.strftime("%Y-%m-%d", time.gmtime(NOW() - 5 * 86400)), time.strftime("%Y-%m-%d", time.gmtime(NOW() + 86400))
    for page in (1, 2, 3):
        j = fetch_json(f"https://api.avalanche.ca/min/en/submissions?fromdate={a}&todate={b}&pagesize=50&page={page}")
        for m in ((j.get("items") or {}).get("data") or []):
            if not (m.get("observations") or {}).get("incident"): continue
            out.append(item(m.get("id"), f"[AVALANCHE INCIDENT REPORT] Canada, {m.get('region') or (m.get('area') or {}).get('name')}: {m.get('title')} ({(m.get('datetime') or '')[:10]})",
                            f"https://avalanche.ca/mountain-information-network/submissions/{m.get('id')}",
                            f"Mountain Information Network report with an incident section. Region: {m.get('region')}. Tags: {m.get('tags')}. "
                            f"Posted by {m.get('username')}.\n\nSource: Avalanche Canada MIN.", iso(m.get("datetime")), "AVALANCHE INVOLVEMENT"))
        if page >= (j.get("totalPages") or 1): break
    return out

def uac(s):
    out = []
    page = fetch("https://utahavalanchecenter.org/avalanches").decode("utf-8", "replace")
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        m = re.search(r'href="(/avalanche/\d+)"[^>]*>(.*?)</a>', tr, re.S)
        tds = [strip(x) for x in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if not m or len(tds) < 4: continue
        trig = tds[3]
        if not re.search(r"skier|snowboard|snowmobil|snow ?bike|hiker|climber|snowshoe|unknown", trig, re.I): continue
        t = local(tds[0], -7)
        if t and NOW() - t > 21 * 86400: continue
        out.append(item(m.group(1), f"[AVALANCHE REPORT] UT, {tds[2]}: {strip(m.group(2))} — trigger: {trig} ({tds[0]})", "https://utahavalanchecenter.org" + m.group(1),
                        f"{strip(m.group(2))}. Region: {tds[2]}. Trigger: {trig}. Depth: {tds[4] if len(tds) > 4 else ''}. Width: {tds[5] if len(tds) > 5 else ''}. "
                        f"Human-triggered avalanche reported to the Utah Avalanche Center; open the report to see whether anyone was caught.\n\nSource: Utah Avalanche Center.", t, "AVALANCHE REPORT"))
    try:
        for n in fetch_json("https://utahavalanchecenter.org/avalanches/fatalities/json").get("nodes", [])[:6]:
            f = n.get("FATALITY") or {}
            t = iso(f.get("Date"))
            if not t or NOW() - t > 45 * 86400: continue
            out.append(item(f"fat|{f.get('URL')}", f"[AVALANCHE FATALITY] UT, {f.get('Region')}: {f.get('Place')} — {f.get('Killed')} killed ({f.get('Date')})", f.get("URL") or "https://utahavalanchecenter.org/avalanches/fatalities",
                            f"{f.get('Place')}, {f.get('Region')}. Trigger: {f.get('Trigger')}. Caught {f.get('Caught')}, carried {f.get('Carried')}, killed {f.get('Killed')}.\n\nSource: Utah Avalanche Center fatalities list.", t, "AVALANCHE FATALITY"))
    except Exception:
        pass
    return out

# ------------------------------------------------------------------ adapter: any RSS/Atom feed (native feeds and news searches)
def rss_in(s):
    out = []
    raw = fetch(s["url"])
    root = ET.fromstring(raw)
    A = "{http://www.w3.org/2005/Atom}"
    nodes = root.findall(".//item") or root.findall(f".//{A}entry")
    for n in nodes[: s.get("max", 60)]:
        g = lambda *names: next((n.findtext(x) for x in names if n.findtext(x)), "") or ""
        title = strip(g("title", A + "title"))
        link = g("link") or next((l.get("href") for l in n.findall(A + "link") if l.get("href")), "")
        body = strip(g("description", A + "summary", A + "content", "{http://purl.org/rss/1.0/modules/content/}encoded"))[:1500]
        when = g("pubDate", A + "updated", A + "published", "{http://www.w3.org/2005/Atom}updated", "{http://purl.org/dc/elements/1.1/}date")
        try: t = email.utils.parsedate_to_datetime(when).timestamp()
        except Exception: t = iso(when)
        if t and NOW() - t > s.get("days", 3) * 86400: continue
        if not title and body: title = short(body, 140)
        blob = f"{title} {body}"
        if s.get("match") and not s["match"].search(title if s.get("match_title") else blob): continue
        if s.get("nomatch") and s["nomatch"].search(blob): continue
        if s.get("judge"):
            v = judge("", blob, routes=s.get("routes"))
            if not v: continue
        src = strip(g("source"))
        key = re.sub(r"[^a-z0-9]+", "", title.lower())[:70] if s.get("by_title") else (g("guid", A + "id") or link or title)
        if s.get("status_feed"):   # same guid every time (one row per pass): a changed restriction is a new item
            hit = re.search(r"[^.:]*\b(closed|closure|avalanche|required)\b[^.]*", body, re.I)
            key, title = f"{key}|{h8(re.sub(r'[0-9]+', '', body))}", f"{title}: {short(hit.group(0) if hit else body, 150)}"
        out.append(item(key, f"{s.get('tag', '')}{title}", link, f"{body}\n\n{('Outlet: ' + src + '. ') if src else ''}Source: {s['name']}.", t, s.get("kind", "NEWS")))
    return out

def gnrelay(s):
    """Re-fetch the Google News searches in the NetNewsWire subscription list, slowly, one at a time.
    NetNewsWire asks for all of them at once and Google answers with errors; a slow trickle gets through."""
    path = os.environ.get("SBFF_OPML") or os.path.join(HERE, "..", "..", "Subscriptions.opml")
    if not os.path.exists(path): return []
    urls = {}
    for m in re.finditer(r'<outline[^>]*?(?:text|title)="([^"]*)"[^>]*?xmlUrl="(https://news\.google\.com/rss/search[^"]*)"', open(path, encoding="utf-8").read()):
        urls[html.unescape(m.group(2))] = html.unescape(m.group(1))
    out, fails, got = [], 0, 0
    for u, name in urls.items():
        try:
            out += rss_in(dict(url=u, name=f"Google News search “{name}”", by_title=True, days=2, max=30))
            fails, got = 0, got + 1
        except Exception:
            fails += 1
            if fails >= 6: break
            time.sleep(8)
            continue
        time.sleep(float(os.environ.get("SBFF_GN_DELAY", "1.5")))
    if urls and not got: raise RuntimeError("Google News refused every request")
    s["_note"] = f"{got} of {len(urls)} searches answered"
    return out

# ------------------------------------------------------------------ adapter: pages with no feed (a new link on the page = a new item)
WATCH_SKIP = re.compile(r"^(mailto:|tel:|javascript:|#)|facebook\.com|twitter\.com|x\.com/|instagram\.com|youtube\.com|linkedin\.com|tiktok\.com|"
                        r"/(login|sign-?in|search|contact|privacy|terms|accessibility|careers|jobs|cart|account)\b", re.I)
def pagewatch(s):
    """Lists the links on a news, notices or incident page. The page's existing links are only remembered on the first run;
    after that, any link that appears on the page is posted (a new press release, incident notice, agenda or report)."""
    page = fetch(s["url"]).decode("utf-8", "replace")
    page = re.sub(r"<(header|footer|nav|script|style|noscript)\b[^>]*>.*?</\1>", " ", page, flags=re.S | re.I)
    out, seen, raw = [], set(), 0
    for href, text in re.findall(r'<a\b[^>]*?href="([^"]+)"[^>]*>(.*?)</a>', page, re.S | re.I):
        href, text = html.unescape(href).strip(), strip(text)
        if WATCH_SKIP.search(href) or len(text) < s.get("min", 18): continue
        if re.fullmatch(r"(read|learn|see|view|find out) (more|all|details)|read (the )?(full )?(post|story|article)|continue reading|full story|more news|next|previous|page \d+|\d+", text, re.I): continue
        url = urllib.parse.urljoin(s["url"], href).split("#")[0]
        if url.rstrip("/") == s["url"].rstrip("/") or url in seen: continue
        raw += 1
        if s.get("link") and not s["link"].search(url): continue
        if s.get("match") and not s["match"].search(text + " " + url): continue
        seen.add(url)
        out.append(item(url, f"[{s['tag']}] {s['who']}: {short(text, 160)}", url,
                        f"New link on {s['who']}'s page {s['url']}: “{text}”.\n\nThis page has no RSS feed, so the feed factory watches it for new links. "
                        f"Open the link to read the release or notice and confirm its date.\n\nSource: {s['name']}.", None, s.get("kind", "PAGE UPDATE")))
        if len(out) >= s.get("max", 60): break
    if not raw: raise RuntimeError("no links found (page may now load by JavaScript, or its layout changed)")
    return out

# ------------------------------------------------------------------ adapter: new studies in PubMed (NCBI E-utilities, free, no key needed at this rate)
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
def pubmed(s):
    """New PubMed records for a search. The first run only remembers what is there; after that, each newly indexed study is posted."""
    q = urllib.parse.quote(s["term"])
    es = fetch_json(f"{EUTILS}esearch.fcgi?db=pubmed&retmode=json&sort=pub_date&datetype=edat&reldate=30&retmax=30&tool=snowbrains-feedfactory&term={q}")
    r = es.get("esearchresult") or {}
    if "ERROR" in r: raise RuntimeError(str(r["ERROR"])[:120])
    ids = r.get("idlist") or []
    if not ids: return []
    time.sleep(1)
    sm = fetch_json(f"{EUTILS}esummary.fcgi?db=pubmed&retmode=json&tool=snowbrains-feedfactory&id={','.join(ids)}").get("result") or {}
    out = []
    for uid in sm.get("uids", []):
        d = sm.get(uid) or {}
        title = strip(d.get("title"))
        if not title: continue
        journal = d.get("fulljournalname") or d.get("source") or "a journal"
        names = [a.get("name") for a in d.get("authors", []) if a.get("name")]
        who = ", ".join(names[:3]) + (" et al." if len(names) > 3 else "")
        when = d.get("epubdate") or d.get("pubdate") or ""
        link = f"https://pubmed.ncbi.nlm.nih.gov/{uid}/"
        out.append(item(f"pmid{uid}", f"[RESEARCH] {s['who']}: {short(title, 180)}", link,
                        f"New study indexed in PubMed: “{title}”. {journal}, {when}. {('Authors: ' + who + '.') if who else ''}\n\n"
                        f"Open the PubMed page for the abstract and a link to the full paper (often free), and check the publication date before treating it as news. "
                        f"A study is a story when its findings are new and interesting to skiers and snowboarders (injury risks, helmets, avalanche survival, snow and climate).\n\nSource: {s['name']}.",
                        None, "RESEARCH"))
    return out

# ------------------------------------------------------------------ sources
R = re.compile
def IBI(i, st, name, host, group, utc, routes=None, layers=None, **kw):
    d = dict(id=i, st=st, name=name, host=host, group=group, utc=utc, routes=R(routes, re.I) if routes else None, fn=ibi, **kw)
    if layers: d["layers"] = layers
    return d
GN = lambda q: "https://news.google.com/rss/search?q=" + urllib.parse.quote_plus(q + " when:1d") + "&hl=en-US&gl=US&ceid=US:en"
BING = lambda q: "https://www.bing.com/news/search?q=" + urllib.parse.quote_plus(q) + "&format=rss&qft=sortbydate%3d%221%22"
BSKY = lambda h: f"https://bsky.app/profile/{h}/rss"
def FEED(i, name, url, group="native", **kw): return dict(id=i, name=name, url=url, group=group, fn=rss_in, **kw)
def SEARCH(i, q, engine=GN): return dict(id=i, name=f"News search: {q}", url=engine(q), group="search", fn=rss_in, by_title=True, days=2, every=1700, max=40)
ROADNEWS = R(r"clos|reopen|avalanche|slide|chain|traction|pass\b|canyon|winter|snow|storm", re.I)

SOURCES = [
 # ---- US West
 dict(id="ca-roads", name="Caltrans highway conditions (Sierra and SoCal mountain routes)", group="west", fn=caltrans_roads,
      routes=[80, 50, 88, 89, 120, 108, 4, 203, 395, 267, 28, 2, 18, 330, 38, 168, 158, 44, 36]),
 dict(id="ca-d9", name="Caltrans District 9 road status (US-395, Tioga, Sonora, Monitor)", group="west", fn=arcgis, st="CA", site="https://dot.ca.gov/caltrans-near-me/district-9",
      url="https://services1.arcgis.com/8CpMUd3fdw6aXef7/ArcGIS/rest/services/D9_Road_Status_Public/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=json",
      name_f="SEGNAME", status_f="SEGSTATUS", route_f="ROUTE", codes={1: "Open", 2: "Chain control R1", 3: "Chain control R2", 4: "Closed"}, note_f=["Notes"]),
 dict(id="or-tripcheck", name="Oregon TripCheck", group="west", fn=tripcheck),
 IBI("nv-511", "NV", "Nevada 511 (NVroads)", "www.nvroads.com", "west", -8,
     r"SR-?431|Mt\.? Rose|US-?50|SR-?28\b|SR-?207|Kingsbury|I-80|SR-?88|SR-?89|US-?395|SR-?156|SR-?157|Lee Canyon|Kyle Canyon|SR-?227|Lamoille|SR-?341"),
 IBI("ak-511", "AK", "Alaska 511", "511.alaska.gov", "west", -9,
     r"Seward|Glenn|Richardson|Parks|Sterling|Hatcher|Alyeska|Haines|Klondike|Thane|Egan|Dalton|Steese|Taylor|Edgerton|Copper River|Portage", ["Closures", "Incidents", "WeatherIncidents"], state="Alaska"),
 FEED("wa-passes", "WSDOT mountain pass conditions", "https://wsdot.wa.gov/traffic/api/MountainPassConditions/rss.aspx", "west",
      match=R(r"closed|closure|avalanche|chains required|traction tires required", re.I), tag="[WA] ", kind="CLOSED", days=2, status_feed=True),
 FEED("wa-alerts", "WSDOT highway alerts", "https://wsdot.wa.gov/traffic/api/HighwayAlerts/rss.aspx", "west", judge=True, tag="[WA] ", kind="CLOSED", max=200,
      routes=R(r"I-90|US ?2\b|US ?12\b|SR ?410|SR ?20\b|SR ?542|SR ?123|SR ?504|US ?97|SR ?207|SR ?906|SR ?706", re.I)),
 # ---- US Rockies / Southwest
 dict(id="co-cotrip", name="Colorado COtrip", group="rockies", fn=cars, st="CO", host="cotg.carsprogram.org", site="https://www.cotrip.org/"),
 dict(id="wy-wydot", name="WYDOT closures", group="rockies", fn=wydot),
 dict(id="mt-mdt", name="Montana DOT events", group="rockies", fn=mdt),
 IBI("id-511", "ID", "Idaho 511", "511.idaho.gov", "rockies", -7,
     r"SH-?21\b|SH-?75|SH-?55|US-?12\b|US-?93|US-?95|US-?20\b|US-?26|SH-?33|SH-?31|SH-?32|I-90|SH-?200|US-?2\b|US-?89|SH-?3\b|SH-?5\b|SH-?6\b|Bogus Basin|Galena|Lolo|Teton",
     ["ActiveClosures", "Incidents"]),
 IBI("ut-udot", "UT", "UDOT Traffic", "www.udottraffic.utah.gov", "rockies", -7, us_only=True, routes=
     r"SR-?210|SR-?190|SR-?224|SR-?248|I-80|US-?40|SR-?92|SR-?39|SR-?158|SR-?226|US-?89|SR-?143|SR-?148|SR-?14\b|SR-?150|US-?189|SR-?35|SR-?65|Cottonwood|Parleys|Provo Canyon|Logan Canyon|Ogden Canyon"),
 IBI("az-511", "AZ", "Arizona 511", "www.az511.gov", "rockies", -7, r"US-?180|SR-?260|SR-?273|SR-?366|SR-?67|US-?89A|Snowbowl|SR-?87\b|SR-?89A|Catalina|Mt\.? Lemmon"),
 FEED("co-passes", "CDOT seasonal pass closures and openings", "https://www.codot.gov/news/feeds/seasonalpasscloseopen/rss.xml", "rockies", tag="[CO] ", kind="CLOSED", days=4),
 FEED("co-news", "CDOT statewide news", "https://www.codot.gov/news/feeds/statewide/rss.xml", match=ROADNEWS, tag="[CO] ", days=4),
 FEED("co-cv", "CDOT road closure news", "https://www.codot.gov/news/feeds/comm_vehicle/rss.xml", match=ROADNEWS, tag="[CO] ", days=4),
 FEED("ut-news", "UDOT news", "https://connect.udot.utah.gov/feed/", match=ROADNEWS, tag="[UT] ", days=4),
 FEED("nm-news", "NMDOT news", "https://www.dot.nm.gov/feed/", match=ROADNEWS, tag="[NM] ", days=4),
 # ---- US East / Midwest
 dict(id="ne-511", name="New England 511 (VT, NH, ME)", group="east", fn=newengland),
 dict(id="ny-511", name="511NY", group="east", fn=cars511x, st="NY", base="https://api-511x-nysdot.carsprogram.org", site="https://511ny.org/"),
 dict(id="ma-511", name="Mass511", group="east", fn=cars, st="MA", host="matg.carsprogram.org", site="https://mass511.com/", km=15),
 dict(id="mn-511", name="Minnesota 511", group="east", fn=cars, st="MN", host="mntg.carsprogram.org", site="https://511mn.org/", km=15),
 IBI("pa-511", "PA", "511PA", "www.511pa.com", "east", -5),
 IBI("nc-511", "NC", "DriveNC", "drivenc.gov", "east", -5, r"US-?321|US-?221|NC-?105|NC-?184|NC-?194|US-?421|US-?19\b|US-?441|Blue Ridge"),
 IBI("wi-511", "WI", "511 Wisconsin", "511wi.gov", "east", -6, us_only=True),
 IBI("ct-511", "CT", "CTroads", "ctroads.org", "east", -5),
 # ---- Canada
 dict(id="bc-drivebc", name="DriveBC", group="canada", fn=bc),
 IBI("ab-511", "AB", "511 Alberta", "511.alberta.ca", "canada", -7,
     r"HWY-?1\b|HWY-?1A|HWY-?93|HWY-?40\b|HWY-?742|HWY-?66\b|HWY-?68\b|HWY-?11\b|HWY-?16\b|HWY-?3\b|HWY-?774|HWY-?6\b|HWY-?5\b|HWY-?541|Moraine|Lake Louise|Sunshine|Norquay|Bow Valley|Icefields|Kananaskis|Smith-Dorrien|Marmot|Maligne",
     ["Closures", "Incidents", "WeatherIncidents"]),
 IBI("yt-511", "YT", "511 Yukon", "511yukon.ca", "canada", -7, r"Klondike|Haines|Alaska H|Dempster|Atlin|Tagish", state="Yukon"),
 IBI("on-511", "ON", "Ontario 511", "511on.ca", "canada", -5, r"HWY ?26\b|HWY ?400|HWY ?11\b|HWY ?17\b|HWY ?60\b|HWY ?35\b|HWY ?10\b|HWY ?6\b|Blue Mountain|Collingwood"),
 dict(id="qc-511", name="Québec 511", group="canada", fn=quebec),
 IBI("mb-511", "MB", "Manitoba 511", "www.manitoba511.ca", "canada", -6),
 IBI("sk-511", "SK", "Saskatchewan Highway Hotline", "hotline.gov.sk.ca", "canada", -6),
 IBI("nb-511", "NB", "511 New Brunswick", "511.gnb.ca", "canada", -4),
 IBI("ns-511", "NS", "511 Nova Scotia", "511.novascotia.ca", "canada", -4),
 IBI("nl-511", "NL", "511 Newfoundland and Labrador", "511nl.ca", "canada", -3.5),
 # ---- National parks
 dict(id="nps-alerts", name="National Park Service alerts", group="parks", fn=nps, every=6 * 3600,
      parks=["yell", "grte", "glac", "yose", "mora", "romo", "crla", "lavo", "olym", "noca", "seki", "dena"]),
 dict(id="grte-roads", name="Grand Teton road status", group="parks", fn=arcgis, st="NPS GRTE", site="https://www.nps.gov/grte/planyourvisit/roads.htm",
      url="https://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/GRTE_MajorRoadStatus_(PublicView)/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=false&f=json",
      name_f="RDNAME", status_f="CurrentStatus", codes={1: "Open", 2: "Open with restrictions", 3: "Closed"}, note_f=["RoadConditionsEdit", "RoadCondtionsSecondaryEdit", "SEASDESC"], time_f="EditDate_1"),
 # ---- Avalanche
 dict(id="av-accidents", name="US avalanche accident reports (national database, CAIC)", group="avalanche", fn=caic_accidents),
 dict(id="av-caic-caught", name="CAIC field reports: caught in avalanche", group="avalanche", fn=caic_caught),
 dict(id="av-us-map", name="avalanche.org: warnings and HIGH/EXTREME danger, all US centers", group="avalanche", fn=nac_map),
 # avalanche.org's observations service only answers requests that come from an avalanche center's own website, so it is not polled.
 # Ask the National Avalanche Center for access to add it back (the nac_obs reader above is ready).
 dict(id="av-canada", name="Avalanche Canada: danger, warnings, incident reports", group="avalanche", fn=avcan),
 # Utah Avalanche Center's avalanche list refuses automated readers (HTTP 403), so it is not polled; its RSS feed is in NetNewsWire.
 FEED("av-nws", "NWS avalanche warnings, watches and advisories",
      "https://api.weather.gov/alerts/active.atom?event=Avalanche%20Warning,Avalanche%20Watch,Avalanche%20Advisory", "avalanche", tag="[NWS] ", kind="AVALANCHE WARNING", days=3),
]
# Left out (2026-11-16): pages that load only by JavaScript (Alta, Aspen Snowmass, Boyne, Loon, Powder Mountain, POWDR), refuse automated readers
# (Colorado tramway News page, Michigan LARA, NH Fire Marshal), list nothing (Oregon BCD) or moved (Killington, NSAA, Sunshine Village).
# ---- Pages with no RSS: lift regulators, industry newsrooms, Forest Service newsrooms, big resorts' press pages (added 2026-11-16)
LIFT = R(r"tram|ropeway|ski|lift|chair|gondola|amusement|remont", re.I)
SKIFS = R(r"ski|snow|avalanche|winter|lift|resort|recreation|special use|master development|expansion|housing|closure", re.I)
def WATCH(i, tag, who, url, **kw): return dict(id=i, name=f"Page watch: {who}", url=url, tag=tag, who=who, group="watch", fn=pagewatch, every=kw.pop("every", 3300), **kw)
for i, tag, who, url, kw in [
    ("w-co-tramway-home", "LIFT REGULATOR", "Colorado Passenger Tramway Safety Board (home)", "https://dpo.colorado.gov/PassengerTramway", {}),
    ("w-vt-tramway", "LIFT REGULATOR", "Vermont Passenger Tramway Board", "https://labor.vermont.gov/passenger-tramway/passenger-tramway-board", {}),
    ("w-ut-ropeway", "LIFT REGULATOR", "Utah Passenger Ropeway Safety Committee (public notices)", "https://www.utah.gov/pmn/sitemap/publicbody/62.html", {}),
    ("w-me-tramway", "LIFT REGULATOR", "Maine Elevator and Tramway Safety Program", "https://www.maine.gov/pfr/professionallicensing/professions/elevator-tramway-safety-program/home/news-publications", {}),
    ("w-ca-dir", "LIFT REGULATOR", "California DIR / Cal-OSHA (ski lift and tramway items only)", "https://www.dir.ca.gov/DIRNews/", dict(match=LIFT)),
    ("w-on-tssa", "LIFT REGULATOR", "TSSA Ontario newsroom", "https://www.tssa.org/newsroom", dict(match=R(r"\bski(s|ing|er|ers)?\b|ski[- ]lifts?|chair ?lifts?|gondolas?|ropeways?|passenger tramways?|aerial lifts?|t-bars?|surface lifts?|magic carpets?", re.I))),
    ("w-bc-tsbc", "LIFT REGULATOR", "Technical Safety BC incident investigations", "https://www.technicalsafetybc.ca/regulatory-resources/incident-investigations", {}),
    ("w-alterra", "SKI INDUSTRY", "Alterra Mountain Company / Ikon Pass", "https://www.alterramtn.co/en/news", {}),
    ("w-indy", "SKI INDUSTRY", "Indy Pass", "https://www.indyskipass.com/this-is-indy/news", {}),
    ("w-cscusa", "SKI INDUSTRY", "Colorado Ski Country USA", "https://www.coloradoski.com/blog", {}),
    ("w-skiutah", "SKI INDUSTRY", "Ski Utah", "https://www.skiutah.com/news", {}),
    ("w-skivt", "SKI INDUSTRY", "Ski Vermont press room", "https://skivermont.com/news-feed/?category=Press+Room", {}),
    ("w-skinh-news", "SKI INDUSTRY", "Ski NH", "https://www.skinh.com/news", dict(link=R(r"/news/[a-z0-9-]+", re.I))),
    ("w-skiny", "SKI INDUSTRY", "Ski Areas of New York", "https://www.iskiny.com/news/", {}),
    ("w-skise", "SKI INDUSTRY", "Ski Southeast", "https://www.skisoutheast.com/news/", {}),
    ("w-fs-r1", "FOREST SERVICE", "Forest Service Northern Region (MT, northern ID)", "https://www.fs.usda.gov/r01/newsroom/releases", dict(match=SKIFS)),
    ("w-fs-r2", "FOREST SERVICE", "Forest Service Rocky Mountain Region (CO, WY)", "https://www.fs.usda.gov/r02/newsroom/releases", dict(match=SKIFS)),
    ("w-fs-r4", "FOREST SERVICE", "Forest Service Intermountain Region (UT, ID, NV, WY)", "https://www.fs.usda.gov/r04/newsroom/releases", dict(match=SKIFS)),
    ("w-fs-r5", "FOREST SERVICE", "Forest Service Pacific Southwest Region (CA)", "https://www.fs.usda.gov/r05/newsroom/releases", dict(match=SKIFS)),
    ("w-fs-r6", "FOREST SERVICE", "Forest Service Pacific Northwest Region (OR, WA)", "https://www.fs.usda.gov/r06/newsroom/releases", dict(match=SKIFS)),
    ("w-fs-r10", "FOREST SERVICE", "Forest Service Alaska Region", "https://www.fs.usda.gov/r10/newsroom/releases", dict(match=SKIFS)),
    ("w-fs-whiteriver", "FOREST SERVICE", "White River National Forest (Vail, Aspen, Summit County)", "https://www.fs.usda.gov/r02/whiteriver/newsroom/releases", {}),
    ("w-fs-mthood", "FOREST SERVICE", "Mt. Hood National Forest", "https://www.fs.usda.gov/r06/mthood/newsroom/releases", dict(match=R(r"\bski(s|ing|er|ers)?\b|ski area|snowpack|snowfall|avalanche|timberline|mt\.? hood meadows|skibowl|cooper spur|special use permit|master development plan|chairlifts?|\blifts?\b|sno-?parks?", re.I))),
    ("w-copper", "RESORT NEWS", "Copper Mountain", "https://www.coppercolorado.com/press-room/press-releases/latest-press-releases/", {}),
    ("w-abasin", "RESORT NEWS", "Arapahoe Basin", "https://www.arapahoebasin.com/als-blog/", dict(link=R(r"/als-blog/[^/]+/?$", re.I), min=6)),
    ("w-snowbird", "RESORT NEWS", "Snowbird", "https://www.snowbird.com/community/press-releases/", {}),
    ("w-deervalley", "RESORT NEWS", "Deer Valley", "https://www.deervalley.com/media-room", {}),
    ("w-snowbasin", "RESORT NEWS", "Snowbasin", "https://www.snowbasin.com/blog/news", {}),
    ("w-jackson", "RESORT NEWS", "Jackson Hole", "https://www.jacksonhole.com/blog", {}),
    ("w-bigsky", "RESORT NEWS", "Big Sky", "https://www.bigskyresort.com/press-kit", {}),
    ("w-sunvalley", "RESORT NEWS", "Sun Valley", "https://www.sunvalley.com/blog/news", {}),
    ("w-mammoth", "RESORT NEWS", "Mammoth Mountain", "https://www.mammothmountain.com/discover-mammoth/mammoth-blog", {}),
    ("w-sugarbowl", "RESORT NEWS", "Sugar Bowl", "https://www.sugarbowl.com/thedrift", {}),
    ("w-taos", "RESORT NEWS", "Taos Ski Valley", "https://www.skitaos.com/blog", {}),
    ("w-revelstoke", "RESORT NEWS", "Revelstoke Mountain Resort", "https://www.revelstokemountainresort.com/whats-happening/", {}),
    ("w-bigwhite", "RESORT NEWS", "Big White", "https://www.bigwhite.com/explore-big-white/blog", dict(link=R(r"/blog/[^/]+/[^/?#]+", re.I))),
    ("w-bachelor", "RESORT NEWS", "Mt. Bachelor", "https://www.mtbachelor.com/culture/blog/", {}),
]:
    SOURCES.append(WATCH(i, tag, who, url, **kw))
# ---- Press releases, ski-trade pages with no RSS, and new studies (added 2026-11-18). All go into sbff-watch.xml except the four news searches.
SKIBIZ = R(r"^(?!.*(?:CAGR|market (?:size|share|research|report|analysis)|forecast period|research and markets)).*\b(?:ski|skis|skiing|skier|skiers|snowboard\w*|ski resorts?|ski areas?|chairlifts?|gondolas?|lift tickets?|season pass(?:es)?|Epic Pass|Ikon Pass|Indy Pass|avalanche\w*|snowmaking|apr[eè]s-ski|heli-?ski\w*|backcountry|SkiStar|Vail Resorts|Alterra|Boyne|POWDR|Aspen Skiing|Snowbird|Mountain Capital Partners)\b", re.I | re.S)
for i, who, short_who, url in [
    ("pr-prn-travel", "PR Newswire travel releases (ski items only)", "PR Newswire", "https://www.prnewswire.com/rss/travel-latest-news/travel-latest-news-list.rss"),
    ("pr-prn-sports", "PR Newswire sports releases (ski items only)", "PR Newswire", "https://www.prnewswire.com/rss/sports-latest-news/sports-latest-news-list.rss"),
]:
    SOURCES.append(dict(id=i, name=who, url=url, group="watch", fn=rss_in, match=SKIBIZ, match_title=True, nomatch=R(r"Colorado Avalanche|\bNHL\b|hockey|private aviation|private jet", re.I), days=4, every=3300, kind="PRESS RELEASE",
                        tag=f"[SKI INDUSTRY] Press release ({short_who}): "))
for i, tag, who, url, kw in [
    ("w-ne-ski-industry", "SKI INDUSTRY", "New England Ski Industry", "https://www.newenglandskiindustry.com/indexarchives.php", dict(link=R(r"viewstory\.php", re.I))),
    ("w-world-ski-awards", "SKI INDUSTRY", "World Ski Awards", "https://worldskiawards.com/news", dict(link=R(r"/news/article/", re.I), every=6 * 3600)),
]:
    SOURCES.append(WATCH(i, tag, who, url, **kw))
for i, who, term in [
    ("pm-ski-snowboard", "Skiing and snowboarding studies", '(skiing[tiab] OR skier[tiab] OR skiers[tiab] OR snowboard*[tiab] OR "ski resort"[tiab] OR "ski area"[tiab] OR "ski patrol"[tiab])'),
    ("pm-avalanche", "Avalanche studies", '(avalanche*[tiab] AND (snow[tiab] OR burial[tiab] OR buried[tiab] OR rescue[tiab] OR victim*[tiab] OR skier*[tiab] OR mountain*[tiab]))'),
    ("pm-snow-climate", "Snow and mountain-climate studies", '((snowpack[tiab] OR "snow cover"[tiab] OR "snow drought"[tiab] OR snowfall[tiab] OR glacier*[tiab]) AND (climate[tiab] OR warming[tiab]) AND (mountain*[tiab] OR alpine[tiab] OR ski*[tiab]))'),
]:
    SOURCES.append(dict(id=i, name=f"PubMed: {who}", who=who, term=term, group="watch", fn=pubmed, every=6 * 3600, slow=True))
for i, q in [
    ("s-businesswire-ski", 'site:businesswire.com (ski OR skiing OR snowboard OR "ski resort" OR chairlift)'),
    ("s-prnewswire-ski", 'site:prnewswire.com ("ski resort" OR skiing OR "ski area" OR snowboard)'),
    ("s-globenewswire-ski", 'site:globenewswire.com (ski OR skiing OR snowboard OR "ski resort") -market'),   # GlobeNewswire's own RSS refuses the factory (403)
    ("s-fis-news", "site:fis-ski.com"),
    ("s-pistehors", "site:pistehors.com"),
]:
    SOURCES.append(SEARCH(i, q))
for _s in SOURCES:   # Google News ignores the ski words in press-wire site searches, so filter those results here too
    if _s["id"] in ("s-globenewswire-ski", "s-businesswire-ski", "s-prnewswire-ski"): _s["match"] = SKIBIZ
# native feeds NetNewsWire can read directly; the factory also reads them so the cloud scan sees them in all.json
for i, n, u in [
    ("bs-wsdot-passes", "WSDOT Passes (Bluesky)", BSKY("passes.wsdot.wa.gov")), ("bs-wsdot-snoq", "WSDOT Snoqualmie Pass (Bluesky)", BSKY("snoqualmiepass.wsdot.wa.gov")),
    ("bs-wsdot", "WSDOT (Bluesky)", BSKY("wsdot.wa.gov")), ("bs-wsdot-east", "WSDOT East (Bluesky)", BSKY("east.wsdot.wa.gov")), ("bs-wsdot-north", "WSDOT North (Bluesky)", BSKY("north.wsdot.wa.gov")),
    ("bs-i80", "I-80 chains bot (Bluesky)", BSKY("i80chains.bsky.social")), ("bs-us50", "US 50 chains bot (Bluesky)", BSKY("us50chains.bsky.social")),
    ("bs-wv-east", "511 Eastern WV (Bluesky)", BSKY("wv511east.bsky.social")), ("bs-nmdot", "NMDOT (Bluesky)", BSKY("newmexicodot.bsky.social")),
    ("bs-mndot", "MnDOT (Bluesky)", BSKY("mndot.mn.gov")), ("bs-midot", "Michigan DOT (Bluesky)", BSKY("michigandot.bsky.social")), ("bs-medot", "MaineDOT (Bluesky)", BSKY("mainedot.bsky.social")),
    ("bs-bcsar", "BC Search and Rescue Association (Bluesky)", BSKY("bcsara.com")),
    ("ac-cbac", "Crested Butte Avalanche Center", "https://cbavalanchecenter.org/feed/"), ("ac-btac", "Bridger-Teton Avalanche Center", "https://bridgertetonavalanchecenter.org/feed/"),
    ("ac-cnfaic", "Chugach NF Avalanche Center", "https://www.cnfaic.org/feed/"), ("ac-hpac", "Hatcher Pass Avalanche Center", "https://hpavalanche.org/feed/"),
    ("ac-aaic", "Alaska Avalanche Information Center", "https://alaskasnow.org/feed/"), ("ac-wcmac", "West Central Montana Avalanche Center", "https://missoulaavalanche.org/feed/"),
    ("ac-bac", "Bridgeport Avalanche Center", "https://bridgeportavalanchecenter.org/feed/"),
    ("ac-coaa", "Central Oregon Avalanche Center", "https://www.coavalanche.org/blogs/news.atom"), ("ac-aq", "Avalanche Québec", "https://avalanchequebec.ca/feed/"),
    ("sar-alpine", "Alpine Rescue Team (Colorado) missions", "https://www.alpinerescueteam.org/feed/"), ("sar-bcsara", "BC Search and Rescue Association news", "https://bcsara.com/feed/"),
]:
    SOURCES.append(FEED(i, n, u, days=4, every=1700))
for i, q in [
    ("s-snow-avalanche", "snow avalanche"),
    ("s-avalanche-people", "avalanche (skier OR snowboarder OR snowmobiler OR climber OR hiker) (buried OR killed OR caught OR rescued OR injured OR missing)"),
    ("s-avalanche-warning", '"avalanche warning" OR "avalanche danger" OR "avalanche watch"'),
    ("s-avalanche-control", '"avalanche control" OR "avalanche mitigation" OR interlodge (highway OR pass OR canyon OR road)'),
    ("s-pass-closed", "(pass OR canyon OR highway OR interstate) (closed OR closure OR reopens OR reopened) (snow OR avalanche OR rockslide OR mudslide OR blizzard)"),
    ("s-chains", '"chain control" OR "chain law" OR "traction law" (I-80 OR I-70 OR pass OR summit)'),
    ("s-passes-co-wy", '"Teton Pass" OR "Loveland Pass" OR "Berthoud Pass" OR "Red Mountain Pass" OR "Wolf Creek Pass" OR "Vail Pass" OR "Togwotee Pass" (closed OR closure OR reopens OR avalanche)'),
    ("s-passes-ut", '"Little Cottonwood Canyon" OR "Big Cottonwood Canyon" OR "Parleys Canyon" OR "Guardsman Pass" (closed OR closure OR avalanche OR interlodge OR reopens)'),
    ("s-passes-sierra", '"Donner Summit" OR "Donner Pass" OR "Echo Summit" OR "Tioga Pass" OR "Sonora Pass" OR "Carson Pass" OR "Mount Rose Highway" (closed OR closure OR chains OR avalanche OR reopens)'),
    ("s-passes-pnw", '"Snoqualmie Pass" OR "Stevens Pass" OR "White Pass" OR "Chinook Pass" OR "North Cascades Highway" OR "Santiam Pass" OR "Mount Hood" highway (closed OR closure OR avalanche OR reopens)'),
    ("s-passes-canada", '"Rogers Pass" OR Coquihalla OR "Sea to Sky" OR "Kootenay Pass" OR "Icefields Parkway" OR "Trans-Canada" (closed OR closure OR avalanche OR reopens)'),
    ("s-passes-east", '"Smugglers Notch" OR "Kancamagus" OR "Pinkham Notch" OR "Franconia Notch" OR "Whiteface" highway (closed OR closure OR reopens)'),
    ("s-rescue", '("search and rescue" OR sheriff) (skier OR snowboarder OR backcountry OR snowmobiler) (rescued OR missing OR found OR injured)'),
]:
    SOURCES.append(SEARCH(i, q))
# ---- Resorts and agencies whose Facebook pages only show to US/Canada viewers, so Steven's UK-based profile can't follow them (added 2026-11-19).
# Their own feeds and news pages where they have one, plus news searches that name them.
for i, n, u in [
    ("r-nubsnob", "Nub's Nob (MI) blog", "https://www.nubsnob.com/feed/"),
    ("r-snowridge", "Snow Ridge (NY) news", "https://snowridge.com/feed/"),
    ("r-sandiapeak", "Sandia Peak (NM) news", "https://sandiapeak.com/feed/"),
]:
    SOURCES.append(FEED(i, n, u, days=4, every=3300))
# Mount Shasta Avalanche Center moved to a new website on 11/12/26 and dropped its RSS feed; its observations already come in through the avalanche.org feeds (av-*).
SOURCES.append(WATCH("w-msac-blog", "AVALANCHE CENTER", "Mount Shasta Avalanche Center (news and conditions posts)", "https://www.shastaavalanche.org/blog", link=R(r"/blog/[^/?#]+$", re.I)))
SOURCES.append(WATCH("w-snowtrails", "RESORT NEWS", "Snow Trails (OH)", "https://www.snowtrails.com/blog/skiing", link=R(r"/blog/\d{4}/", re.I)))
for i, q in [
    ("s-us-only-resorts", '("Grand Targhee" OR "Bolton Valley" OR "Snow Ridge" OR "Glen Eden" OR Winterplace OR "Snow Trails" OR "Nubs Nob" OR "Nub\'s Nob" OR "Whitecap Mountains") (ski OR skiing OR skier OR snowboard OR resort OR lift OR chairlift)'),
    ("s-us-only-resorts2", '("Ski Martock" OR "Sandia Peak" OR "Thunder Ridge" OR "Oak Mountain" OR "Maple Ski Ridge" OR "Ski Bradford" OR "Snow Snake" OR "Mt. Eyak" OR "Ski Land" OR "Denton Hill") (ski OR skiing OR skier OR snowboard OR "ski area" OR chairlift OR "rope tow")'),
    ("s-us-only-agencies", '("Squamish RCMP" OR "Smithers RCMP" OR "Ridge Meadows RCMP" OR "Flathead County Sheriff" OR "Delta County Sheriff" OR "Montezuma County Sheriff" OR "Juab County Sheriff" OR "Morgan County Sheriff" OR "Unified Police") (rescue OR avalanche OR skier OR snowboarder OR hiker OR climber OR missing)'),
]:
    SOURCES.append(SEARCH(i, q))
# ---- Reddit (added 2026-11-20, Steven's request): snowsports subreddits plus local mountain-town and regional subreddits across North America and Europe.
# Read as multireddit RSS (one request per group). Only posts that look like news reach sbnews-search.xml: snowsports subs need an event word;
# town and regional subs need a ski/snow/mountain word AND an event word. A Reddit post is a tip, not usually the original source.
RD_EVENT = r"avalanche|slide\b|buried|caught|rescu|\bSAR\b|search and rescue|missing|stranded|injur|hospital|died|dead\b|death|killed|fatal|helicopter|life ?flight|accident|incident|collision|derail|deropement|evacuat|stuck on|lift (?:stopped|broke|fell|failure|malfunction)|chair (?:fell|broke|fall)|gondola (?:stopped|fell|broke)|clos(?:ed|ure|ing)|shut ?down|reopen|lawsuit|sued|sold|sale of|acquir|bankrupt|layoff|strike|union|cancel|record|first descent|warning|\bPSA\b|breaking"
RD_SKI = r"\bski|snowboard|skier|resort|chair ?lift|\blift|gondola|\btram\b|avalanche|backcountry|splitboard|couloir|summit|\bpeak\b|\bpass\b|canyon|snow|patrol|mountain"
RD_CORE = R(RD_EVENT, re.I)
RD_TOWN = R(r"^(?=.*(?:" + RD_SKI + r"))(?=.*(?:" + RD_EVENT + r"))", re.I | re.S)
RD = lambda subs: "https://www.reddit.com/r/" + "+".join(subs.split()) + "/new/.rss?limit=100"
def reddit(s):
    """Reads the subreddit groups one request at a time, 20 seconds apart (Reddit answers 429 to bursts). A group that answers 404
    (a subreddit that doesn't exist or went private) is split and its subreddits are read one by one, so one bad name can't block the rest."""
    out, notes, ok = [], [], 0
    for who, subs, m in s["groups"]:
        tries = [subs]
        while tries:
            g = tries.pop(0)
            time.sleep(20)
            try:
                out += rss_in(dict(url=RD(g), name=f"Reddit: {who}", match=m, days=2, max=100, kind="REDDIT", tag=f"[REDDIT {who}] "))
                ok += 1
            except urllib.error.HTTPError as e:
                if e.code in (403, 404) and " " in g: tries += g.split()
                else: notes.append(f"{g.split()[0] if ' ' not in g else who}: HTTP {e.code}")
                if e.code == 429: time.sleep(60)
            except Exception as e:
                notes.append(f"{who}: {str(e)[:40]}")
    if not ok: raise RuntimeError("Reddit refused every request: " + "; ".join(notes[:4]))
    s["_note"] = f"{ok} requests answered" + (("; failed: " + "; ".join(notes[:12])) if notes else "")
    return out
# Switched off 2026-11-20: Reddit answers 429/404 to unauthenticated RSS readers (even r/skiing, one request at a time). Needs Reddit API (OAuth) access to work.
if False: SOURCES.append(dict(id="rd-all", name="Reddit: snowsports and mountain-town subreddits (North America and Europe)", group="search", fn=reddit, every=1700, slow=True, groups=[
    ("snowsports", "skiing snowboarding Backcountry splitboarding icecoast telemark skimo Mountaineering xcountryskiing snowmobiling", RD_CORE),
    ("snowsports", "SkiRacing alpinism skipatrol freeskiing avalanches SkiTouring", RD_CORE),
    ("Colorado and Utah", "Colorado COsnow Breckenridge vail aspen telluride Durango CrestedButte Steamboat SummitCounty SaltLakeCity Utah ParkCity Ogden", RD_TOWN),
    ("California and PNW", "tahoe Truckee MammothLakes Bend Leavenworth Spokane pnwskiing Seattle Portland", RD_TOWN),
    ("Northern Rockies, Alaska, Southwest", "Bozeman Missoula montana Idaho Boise JacksonHole wyoming alaska anchorage Juneau Fairbanks taos NewMexico Flagstaff", RD_TOWN),
    ("Northeast and Midwest", "vermont newhampshire Maine Adirondacks upperpeninsula whitemountains", RD_TOWN),
    ("Canada", "Whistler Revelstoke Banff Canmore Calgary squamish kootenays Nelson Fernie britishcolumbia Kamloops kelowna Quebec", RD_TOWN),
    ("Europe", "Alps Chamonix zermatt Verbier Innsbruck Tirol Austria Switzerland Dolomites Andorra Pyrenees ScottishSkiing norway", RD_TOWN),
]))
SOURCES.append(SEARCH("s-bing-snow-avalanche", "snow avalanche", BING))
SOURCES.append(SEARCH("s-bing-pass-closed", "mountain pass closed snow avalanche", BING))
SOURCES.append(dict(id="gn-relay", name="Google News searches from the NetNewsWire list (relayed slowly)", group="relay", fn=gnrelay, every=3300, slow=True))
BY_ID = {s["id"]: s for s in SOURCES}
GROUPS = {"west": ("sbff-roads-us-west.xml", "Roads: US West (CA, OR, WA, NV, AK)"), "rockies": ("sbff-roads-us-rockies.xml", "Roads: US Rockies and Southwest (CO, UT, WY, MT, ID, AZ, NM)"),
          "east": ("sbff-roads-us-east.xml", "Roads: US East and Midwest"), "canada": ("sbff-roads-canada.xml", "Roads: Canada"),
          "parks": ("sbff-parks.xml", "National park roads"), "avalanche": ("sbff-avalanche.xml", "Avalanche: accidents, involvements, warnings, danger"),
          "search": ("sbnews-search.xml", "News searches: avalanches and pass closures"),
          "relay": ("sbnews-google-relay.xml", "Google News searches from the NetNewsWire list (relayed)"), "native": ("sbff-native.xml", "Native DOT, avalanche-center and rescue feeds (pass-through)"),
          "watch": ("sbff-watch.xml", "Page watch: lift regulators, Forest Service, ski-industry and resort newsrooms, press releases and new studies")}

# ------------------------------------------------------------------ engine
def run_source(s):
    t0 = NOW()
    try:
        its = s["fn"](s)
        seen, out = set(), []
        for it in its:
            if it["key"] in seen: continue
            seen.add(it["key"]); out.append(it)
        return s["id"], out, None, NOW() - t0
    except Exception as ex:
        return s["id"], [], f"{type(ex).__name__}: {str(ex)[:160]}", NOW() - t0

def load(path, default):
    try:
        with open(path, encoding="utf-8") as f: return json.load(f)
    except Exception:
        return default

LOCK = threading.Lock()

def one_pass(out_dir, only=None, write=True, log=print, slow=None):
    os.makedirs(out_dir, exist_ok=True)
    spath = os.path.join(out_dir, "_state.json")
    blank = {"seen": {}, "items": [], "last": {}, "health": {}}
    last = load(spath, blank)["last"]
    t0 = NOW()
    todo = [s for s in SOURCES if (not only or s["id"] in only) and (slow is None or bool(s.get("slow")) == slow)
            and (only or t0 - last.get(s["id"], 0) >= s.get("every", 0))]
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(run_source, todo))
    with LOCK:
        return merge(out_dir, spath, load(spath, blank), results, write, log)

def merge(out_dir, spath, st, results, write, log):
    now, new_total = NOW(), 0
    for sid, its, err, secs in results:
        s = BY_ID[sid]
        h = st["health"].setdefault(sid, {"name": s["name"], "group": s["group"]})
        h.update(name=s["name"], group=s["group"], last_run=int(now), seconds=round(secs, 1))
        if err and s.get("us_only"):
            h.update(ok=True, error=None, fails=0, matched=0, new=0, note="answers connections from the US only: will work from the cloud runner")
            continue
        if err:
            h.update(ok=False, error=err, fails=h.get("fails", 0) + 1)
            log(f"  FAIL {sid}: {err}")
            continue
        seen = st["seen"].setdefault(sid, {})
        boot = not seen and not st["last"].get(sid)
        fresh = 0
        for it in its:
            k = it["key"]
            rec = seen.get(k)
            if rec and now - rec[1] <= GONE_HOURS * 3600:
                rec[1] = now
                continue
            seen[k] = [now, now]
            if boot and not (it["t"] and now - it["t"] <= BOOT_HOURS * 3600): continue   # first run: don't flood with old events
            if it["t"] and now - it["t"] > KEEP_DAYS * 86400 and s["group"] not in ("avalanche",): continue
            st["items"].append({"id": f"{sid}:{h8(k, int(now) if rec else 0)}", "source": sid, "source_name": s["name"], "group": s["group"], "kind": it["kind"],
                                "title": it["title"], "link": it["link"], "body": it["body"], "source_time": int(it["t"]) if it["t"] else None, "first_seen": int(now)})
            fresh += 1
        for k in [k for k, v in seen.items() if now - v[1] > 30 * 86400]: del seen[k]
        st["last"][sid] = now
        h.update(ok=True, error=None, fails=0, last_ok=int(now), matched=len(its), new=fresh, note=s.get("_note"))
        new_total += fresh
        log(f"  ok   {sid}: {len(its)} matched, {fresh} new ({secs:.1f}s)")
    for i in st["items"]:   # follow sources that moved to another feed, forget sources that were retired
        if i["source"] in BY_ID: i["group"] = BY_ID[i["source"]]["group"]
    st["health"] = {k: v for k, v in st["health"].items() if k in BY_ID}
    st["items"] = sorted((i for i in st["items"] if now - i["first_seen"] <= KEEP_DAYS * 86400), key=lambda i: i["first_seen"], reverse=True)[:12000]
    if write:
        write_outputs(out_dir, st)
        tmp = spath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f: json.dump(st, f)
        os.replace(tmp, spath)
    return st, new_total

NEWS_GROUPS = ("search", "relay", "native")   # news items keep their own publication time; agency events use the time we first saw them

def rss_out(title, desc, items):
    o = ['<?xml version="1.0" encoding="UTF-8"?>', '<rss version="2.0"><channel>', f"<title>{escape(title)}</title><link>https://snowbrains.com/</link>",
         f"<description>{escape(desc)}</description><lastBuildDate>{email.utils.formatdate(usegmt=True)}</lastBuildDate><ttl>15</ttl>"]
    for i in items:
        body = escape(i["body"]).replace("\n", "&lt;br&gt;")
        o.append(f"<item><title>{escape(i['title'])}</title><link>{escape(i['link'] or 'https://snowbrains.com/')}</link><guid isPermaLink=\"false\">{escape(i['id'])}</guid>"
                 f"<pubDate>{email.utils.formatdate(i['source_time'] if i['group'] in NEWS_GROUPS and i.get('source_time') else i['first_seen'], usegmt=True)}</pubDate><category>{escape(i['kind'] or '')}</category><description>{body}</description></item>")
    o.append("</channel></rss>")
    return "\n".join(o)

def write_outputs(out_dir, st):
    def put(name, text):
        tmp = os.path.join(out_dir, name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f: f.write(text)
        os.replace(tmp, os.path.join(out_dir, name))
    for g, (fname, title) in GROUPS.items():
        put(fname, rss_out(f"SnowBrains feed factory · {title}", f"{title}. Built by feedfactory {VERSION} from public agency data.", [i for i in st["items"] if i["group"] == g and (g != "relay" or NOW() - i["first_seen"] <= 3 * 86400)][:5000 if g == "relay" else 500]))
    put("all.json", json.dumps({"version": VERSION, "generated": int(NOW()), "items": st["items"]}, ensure_ascii=False))
    bad = {k: v for k, v in st["health"].items() if not v.get("ok")}
    put("status.json", json.dumps({"version": VERSION, "generated": int(NOW()), "sources": len(st["health"]), "failing": len(bad), "health": st["health"]}, indent=1))
    rows = "".join(f"<tr><td>{escape(k)}</td><td>{escape(v.get('name', ''))}</td><td>{'ok' if v.get('ok') else 'FAILING'}</td><td>{v.get('matched', '')}</td>"
                   f"<td>{time.strftime('%m/%d %H:%M', time.gmtime(v.get('last_ok', 0))) if v.get('last_ok') else '-'}</td><td>{escape(v.get('error') or '')}</td></tr>" for k, v in sorted(st["health"].items()))
    feeds = "".join(f'<li><a href="{f}">{f}</a>: {escape(t)}</li>' for f, t in GROUPS.values())
    put("index.html", f"<!doctype html><meta charset=utf-8><title>SnowBrains feed factory</title><body style='font:14px system-ui;margin:2em'><h1>SnowBrains feed factory {VERSION}</h1>"
        f"<p>Updated {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. {len(st['items'])} items in the last {KEEP_DAYS} days. {len(bad)} of {len(st['health'])} sources failing.</p>"
        f"<ul>{feeds}<li><a href='all.json'>all.json</a></li><li><a href='status.json'>status.json</a></li></ul><table border=1 cellpadding=4 style='border-collapse:collapse'>"
        f"<tr><th>id</th><th>source</th><th>state</th><th>matched</th><th>last ok (UTC)</th><th>error</th></tr>{rows}</table>")

def opml(base):
    base = base.rstrip("/")
    o = ['<?xml version="1.0" encoding="UTF-8"?>', '<opml version="1.1"><head><title>SnowBrains feed factory</title></head><body>',
         '<outline text="SB · Feed factory (roads + avalanche)" title="SB · Feed factory (roads + avalanche)">']
    for g, (f, t) in GROUPS.items():
        if g == "native": continue   # NetNewsWire subscribes to those directly (native_additions.opml)
        o.append(f'<outline text="{escape(t)}" title="{escape(t)}" type="rss" xmlUrl="{base}/{f}"/>')
    o.append("</outline></body></opml>")
    return "\n".join(o)

def main():
    a = sys.argv[1:] or ["test"]
    opt = lambda k, d=None: a[a.index(k) + 1] if k in a else d
    out = opt("--out", os.path.join(HERE, "out"))
    only = set(opt("--only", "").split(",")) - {""} or None
    stamp = lambda m: print(time.strftime("%Y-%m-%d %H:%M:%S"), m, flush=True)
    if a[0] == "run":
        st, n = one_pass(out, only, log=stamp)
        stamp(f"pass done: {n} new items, {sum(1 for v in st['health'].values() if not v.get('ok'))} sources failing")
    elif a[0] == "serve":
        port, every = int(opt("--port", 8511)), int(opt("--every", 600))
        os.makedirs(out, exist_ok=True)
        def loop(slow, pause):
            while True:
                try:
                    st, n = one_pass(out, log=lambda m: None, slow=slow)
                    stamp(f"{'slow' if slow else 'main'} pass done: {n} new items, {sum(1 for v in st['health'].values() if not v.get('ok'))} sources failing")
                except Exception as ex:
                    stamp(f"pass crashed: {ex!r}")
                time.sleep(pause)
        born = os.path.getmtime(os.path.abspath(__file__))
        def reloader():   # pick up a new version of this file without anyone restarting the service
            while True:
                time.sleep(60)
                try:
                    if os.path.getmtime(os.path.abspath(__file__)) != born:
                        compile(open(os.path.abspath(__file__), encoding="utf-8").read(), __file__, "exec")
                        with LOCK:
                            stamp("feedfactory.py changed: restarting with the new version")
                            os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)] + sys.argv[1:])
                except SyntaxError as ex:
                    stamp(f"new feedfactory.py has a syntax error, keeping the old one: {ex}")
                except OSError:
                    pass
        threading.Thread(target=reloader, daemon=True).start()
        threading.Thread(target=loop, args=(False, every), daemon=True).start()
        threading.Thread(target=loop, args=(True, 300), daemon=True).start()
        class H(SimpleHTTPRequestHandler):
            def __init__(self, *x, **k): super().__init__(*x, directory=out, **k)
            def log_message(self, *x): pass
            def do_GET(self):
                if self.path.split("?")[0].lstrip("/").startswith("_"): self.send_error(404); return
                super().do_GET()
        stamp(f"feedfactory {VERSION} serving {out} on http://127.0.0.1:{port}/ and refreshing every {every}s")
        ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
    elif a[0] == "list":
        for s in SOURCES: print(f"{s['id']:22} {s['group']:9} {s['name']}")
        print(len(SOURCES), "sources")
    elif a[0] == "opml":
        print(opml(a[1] if len(a) > 1 else "http://127.0.0.1:8511"))
    else:
        for sid, its, err, secs in map(run_source, [s for s in SOURCES if not only or s["id"] in only]):
            print(f"== {sid}: {'ERROR ' + err if err else str(len(its)) + ' items'} ({secs:.1f}s)")
            for it in its[:25]: print("   ", it["title"][:190])

if __name__ == "__main__":
    main()
