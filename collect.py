#!/usr/bin/env python3
"""SnowBrains cloud collector: stands in for NetNewsWire so the hourly scan can run 24/7 without the Mac.

Reads every feed in Subscriptions.opml and keeps new articles in nnw.sqlite, a small database with the SAME
tables and columns NetNewsWire uses (articles, statuses), so nnw_scan.py and the scan's SQL checks work unchanged.
  - Feeds served by the feed factory (http://127.0.0.1:8511/<file>) are read from the feed factory's output folder.
  - Google News searches (news.google.com) are skipped: the feed factory's relay already reads them, slowly.
  - Everything else is fetched directly, politely (conditional GET, honest User-Agent, 24 at a time).
Keeps 3 days of articles, and 60 days for snowbrains.com and competitor feeds (prior-coverage and competitor checks).

  python3 collect.py --opml Subscriptions.opml --ff out --db out/nnw.sqlite [--seed seed.sqlite]
Standard library only.
"""
import argparse, concurrent.futures as cf, email.utils, gzip, hashlib, html, json, os, re, sqlite3, time, zlib
import urllib.error, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone

VERSION = "2026-11-27b"
UA = "Mozilla/5.0 (compatible; SnowBrains-collector/%s; +https://snowbrains.com)" % VERSION
KEEP_DAYS, LONG_DAYS, SEEN_DAYS = 3, 60, 30
LONG_KEEP = ("snowbrains.com", "powder.com", "unofficialnetworks", "tetongravity", "skimag", "planetski", "onthesnow",
             "outsideonline", "fall-line", "welove2ski", "theinertia")
MAX_BODY = 700   # nnw_scan reads the first 600 characters; keeps the database small
NS = {"atom": "http://www.w3.org/2005/Atom", "content": "http://purl.org/rss/1.0/modules/content/",
      "dc": "http://purl.org/dc/elements/1.1/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
      "rss1": "http://purl.org/rss/1.0/", "media": "http://search.yahoo.com/mrss/"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (articleID TEXT NOT NULL PRIMARY KEY, feedID TEXT NOT NULL, uniqueID TEXT NOT NULL,
  title TEXT, contentHTML TEXT, contentText TEXT, url TEXT, externalURL TEXT, summary TEXT, imageURL TEXT, bannerImageURL TEXT,
  datePublished DATE, dateModified DATE, searchRowID INTEGER, markdown TEXT, authors TEXT, attachments TEXT, thumbnailURL TEXT);
CREATE TABLE IF NOT EXISTS statuses (articleID TEXT NOT NULL PRIMARY KEY, read BOOL NOT NULL DEFAULT 0, starred BOOL NOT NULL DEFAULT 0,
  dateArrived DATE NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS feedstate (feedID TEXT PRIMARY KEY, etag TEXT, modified TEXT, last_ok REAL, last_try REAL,
  last_new REAL, error TEXT, items INTEGER);
CREATE TABLE IF NOT EXISTS seen (articleID TEXT PRIMARY KEY, t REAL);
CREATE INDEX IF NOT EXISTS i_feed ON articles(feedID);
CREATE INDEX IF NOT EXISTS i_arr ON statuses(dateArrived);
"""


def opml_feeds(path):
    out = []
    for o in ET.parse(path).getroot().iter("outline"):
        u = (o.get("xmlUrl") or "").strip()
        if u and u not in out:
            out.append(u)
    return out


def fetch(url, etag=None, modified=None, timeout=40):
    h = {"User-Agent": UA, "Accept": "*/*",   # some WordPress/GovDelivery sites answer 406/415 to a narrower Accept
         "Accept-Encoding": "gzip, deflate"}
    if etag: h["If-None-Match"] = etag
    if modified: h["If-Modified-Since"] = modified
    req = urllib.request.Request(url, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read(15_000_000)
            enc = (r.headers.get("Content-Encoding") or "").lower()
            if enc == "gzip" or b[:2] == b"\x1f\x8b": b = gzip.decompress(b)
            elif enc == "deflate": b = zlib.decompress(b, -zlib.MAX_WBITS)
            return 200, b, r.headers.get("ETag"), r.headers.get("Last-Modified")
    except urllib.error.HTTPError as e:
        if e.code == 304: return 304, None, etag, modified
        raise


def ts(s):
    s = (s or "").strip()
    if not s: return None
    try:
        return email.utils.parsedate_to_datetime(s).timestamp()
    except Exception:
        pass
    try:
        s2 = s.replace("Z", "+00:00")
        d = datetime.fromisoformat(s2)
        if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
        return d.timestamp()
    except Exception:
        return None


def txt(el):
    if el is None: return ""
    return ("".join(el.itertext()) if len(el) else (el.text or "")).strip()


def strip_tags(s):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def parse(b):
    """Return list of dicts: uid, title, link, summary, content, pub, mod, author."""
    b = re.sub(rb"^(\xef\xbb\xbf)?\s+", b"", b)
    b = re.sub(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]", b"", b)
    # some feeds declare entities that ElementTree doesn't know
    b = re.sub(rb"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", b"&amp;", b)
    root = ET.fromstring(b)
    tag = root.tag.split("}")[-1].lower()
    items = []
    if tag == "feed":  # Atom
        for e in root.findall("atom:entry", NS):
            link = ""
            for l in e.findall("atom:link", NS):
                if l.get("rel", "alternate") == "alternate": link = l.get("href", ""); break
            if not link and e.find("atom:link", NS) is not None: link = e.find("atom:link", NS).get("href", "")
            a = e.find("atom:author/atom:name", NS)
            items.append(dict(uid=txt(e.find("atom:id", NS)) or link, title=txt(e.find("atom:title", NS)), link=link,
                              summary=txt(e.find("atom:summary", NS)), content=txt(e.find("atom:content", NS)),
                              pub=ts(txt(e.find("atom:published", NS))) or ts(txt(e.find("atom:updated", NS))),
                              mod=ts(txt(e.find("atom:updated", NS))), author=txt(a)))
    else:  # RSS 2.0 / RSS 1.0 (RDF)
        nodes = root.findall("./channel/item") or root.findall("rss1:item", NS) or root.findall(".//item")
        for e in nodes:
            g = lambda p: txt(e.find(p, NS)) if ":" in p else txt(e.find(p))
            link = g("link") or g("rss1:link") or (e.get("{%s}about" % NS["rdf"]) or "")
            title = g("title") or g("rss1:title")
            desc = g("description") or g("rss1:description")
            items.append(dict(uid=g("guid") or link or (title + desc[:80]), title=title, link=link, summary=desc,
                              content=g("content:encoded"), pub=ts(g("pubDate")) or ts(g("dc:date")), mod=None,
                              author=g("dc:creator") or g("author")))
    return items


def collect(args):
    os.makedirs(os.path.dirname(os.path.abspath(args.db)) or ".", exist_ok=True)
    fresh = not os.path.exists(args.db)
    db = sqlite3.connect(args.db)
    db.executescript(SCHEMA)
    if fresh and args.seed and os.path.exists(args.seed):
        db.execute("ATTACH ? AS s", (args.seed,))
        db.execute("INSERT OR IGNORE INTO articles SELECT * FROM s.articles")
        db.execute("INSERT OR IGNORE INTO statuses SELECT * FROM s.statuses")
        db.execute("INSERT OR IGNORE INTO seen SELECT articleID, dateArrived FROM s.statuses")
        db.commit(); db.execute("DETACH s")
    state = {r[0]: r for r in db.execute("select feedID, etag, modified from feedstate")}
    known = {r[0] for r in db.execute("select feedID from feedstate where last_ok is not null")}
    feeds = opml_feeds(args.opml)
    now = time.time()
    jobs, skipped = [], 0
    for u in feeds:
        if "news.google.com" in u: skipped += 1; continue
        if "127.0.0.1:8511/" in u: jobs.append((u, "local", os.path.join(args.ff, u.split("8511/", 1)[1]))); continue
        jobs.append((u, "web", None))

    def host(u):
        if "/search/?f=rss" in u: return "townnews"   # BLOX/TownNews papers share a backend that throttles bursts
        return urllib.parse.urlsplit(u).netloc.lower()

    GAP = {"townnews": 4.0, "www.reddit.com": 4.0, "old.reddit.com": 4.0}

    def one(job):
        u, kind, path = job
        r = work(job)
        if r[1] == "err" and re.search(r"429|timed out|Timeout|503|502", r[5] or ""):
            time.sleep(12); r = work(job)   # one retry after a pause
        return r

    def lane(js):
        out = []
        for i, j in enumerate(js):
            if i: time.sleep(GAP.get(host(j[0]), 1.0))
            out.append(one(j))
        return out

    def work(job):
        u, kind, path = job
        try:
            if kind == "local":
                if not os.path.exists(path): return u, "err", None, None, None, "feed factory file missing"
                return u, 200, open(path, "rb").read(), None, None, None
            st = state.get(u)
            code, b, et, mo = fetch(u, st[1] if st else None, st[2] if st else None)
            return u, code, b, et, mo, None
        except Exception as e:
            return u, "err", None, None, None, ("%s: %s" % (type(e).__name__, e))[:160]

    new_total, ok, notmod, bad = 0, 0, 0, 0
    lanes = {}
    for j in jobs: lanes.setdefault(host(j[0]) if j[1] == "web" else "local:" + j[0], []).append(j)
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = [r for rs in ex.map(lane, sorted(lanes.values(), key=len, reverse=True)) for r in rs]
    if True:
        for u, code, b, et, mo, err in results:
            n = 0
            if code == 200:
                try:
                    for it in parse(b):
                        uid = (it["uid"] or "").strip()
                        if not uid: continue
                        aid = hashlib.md5((u + "\n" + uid).encode()).hexdigest()
                        if db.execute("select 1 from seen where articleID=?", (aid,)).fetchone(): continue
                        db.execute("insert into seen values (?,?)", (aid, now))
                        content = it["content"] or ""
                        summ = it["summary"] or ""
                        db.execute("insert into articles (articleID, feedID, uniqueID, title, contentHTML, contentText, url, externalURL,"
                                   " summary, datePublished, dateModified, authors) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                                   (aid, u, uid, strip_tags(it["title"])[:500], None, strip_tags(content)[:MAX_BODY],
                                    it["link"], None, strip_tags(summ)[:MAX_BODY], it["pub"] or now, it["mod"],
                                    json.dumps([{"name": it["author"]}]) if it["author"] else None))
                        # first run (or a feed seen for the first time): what's already in the feed isn't news that just arrived,
                        # so date it by publication; afterwards, arrival = now, exactly like NetNewsWire
                        arr = min(now, it["pub"] or now) if (fresh or u not in known) else now
                        db.execute("insert or replace into statuses (articleID, read, starred, dateArrived) values (?,0,0,?)", (aid, arr))
                        n += 1
                    ok += 1
                except Exception as e:
                    code, err = "err", ("parse: %s" % e)[:160]
            elif code == 304:
                notmod += 1
            if code == "err": bad += 1
            new_total += n
            db.execute("insert into feedstate (feedID, etag, modified, last_ok, last_try, last_new, error, items) values (?,?,?,?,?,?,?,?) "
                       "on conflict(feedID) do update set etag=coalesce(excluded.etag, etag), modified=coalesce(excluded.modified, modified), "
                       "last_ok=case when excluded.error is null then excluded.last_try else last_ok end, last_try=excluded.last_try, "
                       "last_new=case when excluded.items>0 then excluded.last_try else last_new end, error=excluded.error, items=excluded.items",
                       (u, et, mo, now if not err else None, now, now if n else None, err, n))
    # prune
    long_clause = " or ".join("a.feedID like '%%%s%%'" % k for k in LONG_KEEP)
    old = db.execute("select a.articleID from articles a join statuses s using(articleID) where s.dateArrived < ? and not (%s)"
                     % long_clause, (now - KEEP_DAYS * 86400,)).fetchall()
    old += db.execute("select a.articleID from articles a join statuses s using(articleID) where s.dateArrived < ?",
                      (now - LONG_DAYS * 86400,)).fetchall()
    db.executemany("delete from articles where articleID=?", old)
    db.executemany("delete from statuses where articleID=?", old)
    db.execute("delete from seen where t < ?", (now - SEEN_DAYS * 86400,))
    db.execute("delete from feedstate where feedID not in (%s)" % ",".join("?" * len(feeds)), feeds)
    db.commit()
    db.execute("VACUUM"); db.close()
    rep = dict(version=VERSION, generated=now, generated_utc=datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               feeds_in_opml=len(feeds), google_news_skipped=skipped, fetched=len(jobs), ok=ok, not_modified=notmod,
               failed=bad, new_articles=new_total, seeded=bool(fresh and args.seed and os.path.exists(args.seed)))
    json.dump(rep, open(os.path.join(os.path.dirname(os.path.abspath(args.db)), "collect_status.json"), "w"), indent=1)
    print(json.dumps(rep))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--opml", default="Subscriptions.opml")
    ap.add_argument("--ff", default="out")
    ap.add_argument("--db", default="out/nnw.sqlite")
    ap.add_argument("--seed", default="seed.sqlite")
    ap.add_argument("--workers", type=int, default=24)
    collect(ap.parse_args())
