#!/usr/bin/env python3
"""
Doppler v0.2 - the Gap Scanner (demand-first)
=============================================
Finds "make this next" opportunities for Doppel Sounds.

What changed from v0.1 (per Kalab's feedback):
  * SEARCH DEMAND IS NOW THE SPINE. "Unreleased" and "low competition" no
    longer score on their own - a festival ID with no search demand is worth
    nothing. Gap + competition are MULTIPLIERS on demand, they can boost or
    suppress but never manufacture a winner. (Encodes the lesson from the
    Hardwell/Marshmello/Illenium IDs that underperformed despite huge set views.)
  * YouTube Data API is the demand engine. One search per candidate does double
    duty: big-view "real" videos = demand; titles with remake/flp/remix = your
    competition. Quota-efficient (~1 search per candidate).
  * Release check moved to MusicBrainz (free, no key, no login, no ban surface)
    instead of the now-restricted Spotify API.
  * New GENRE module: ranks EDM subgenres by YouTube activity so you can spot a
    rising genre and build a remake PACK in it (your longer-video strategy).
  * Hardened candidate parser (the "sacha destiny" collab-noise bug). Also,
    malformed candidates self-filter: a garbled track gets a low demand score
    and sinks, so we don't depend on the parser being perfect.

Reliability model unchanged: every network call isolated, retried x3 w/ backoff,
never raises. Dry-run on mock data is the DEFAULT. --go hits live APIs.
Snapshots persist to SQLite so velocity (acceleration) works from run 2 on.

Keys (env vars; tool degrades without them):
  YT_API_KEY   YouTube Data API v3 key   (demand + competition)

Usage:
  python3 doppler.py                 # dry-run on mock data (safe, no keys)
  python3 doppler.py --go            # LIVE (needs YT_API_KEY for full signal)
  python3 doppler.py --go --genres   # also run the genre-pulse module
  python3 doppler.py --go --max-yt-searches 60 --top 30
"""

import argparse
import datetime as dt
import json
import math
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request

# --------------------------------------------------------------------------
# CONFIG  (edit freely)
# --------------------------------------------------------------------------

WATCHLIST = [
    "Martin Garrix", "Hardwell", "Alesso", "Illenium", "Anyma",
    "John Summit", "Sebastian Ingrosso", "Seth Hills", "W&W",
    "Marshmello", "David Guetta", "Tiesto", "Dimitri Vegas",
    "Swedish House Mafia", "Dom Dolla",
]

GENRE_WATCHLIST = [
    "big room", "future rave", "festival edm", "phonk", "brazilian phonk",
    "hardstyle", "afro house", "melodic techno", "progressive house", "tech house",
    "slap house", "future house", "bass house", "dubstep", "hard techno",
]

# DEMAND-FIRST. Additive base = demand + velocity (demand is the majority, per
# Kalab's "search result 65% at least"). buyer_intent is now a strong,
# rank-sensitive MULTIPLIER, not an additive term (see buyer_intent_mult).
WEIGHTS = {"demand": 0.70, "velocity": 0.30}
GAP_BOOST, GAP_NEUTRAL, GAP_RELEASED = 1.25, 1.0, 0.9
COMP_MIN, COMP_MAX = 0.7, 1.2
# buyer-intent multiplier: up to BI_MAX when remake/flp/instrumental is the TOP
# autocomplete suggestion; decays as it ranks lower. This is the biggest lever.
BI_NEUTRAL, BI_MAX = 1.0, 1.7

# High-value intent: searchers who type these want exactly what you sell (FLPs).
HIGH_INTENT = ["remake", "flp", "fl studio", "instrumental", "remix", "edit", "template", "acapella", "stems"]
LOW_INTENT = ["tutorial", "how to make", "cover", "midi"]
REMAKE_MARKERS = re.compile(r"\b(remake|flp|remix|cover|edit|rebuild|remade|template|acapella)\b", re.I)
COLLAB_LEADERS = {"&", "x", "vs", "feat", "feat.", "ft", "ft.", "with", "and", "b2b", ","}
GENERIC_TAILS = {"official", "live", "mix", "songs", "music", "video", "lyrics",
                 "audio", "remix", "2024", "2025", "2026", "new song", "new"}
# trailing search-modifier words to strip off a candidate ("forever lyrics" -> "forever")
TRAILING_NOISE = {"lyrics", "lyric", "official", "video", "audio", "visualizer", "hd", "4k", "mv", "live"}

HTTP_TIMEOUT, HTTP_RETRIES, HTTP_BACKOFF = 12, 3, 0.6
AUTOCOMPLETE_SLEEP, MUSICBRAINZ_SLEEP = 0.35, 1.1
# MusicBrainz REQUIRES a descriptive UA with contact info. Put your real email.
MB_USER_AGENT = "DoppelSoundsDoppler/0.2 (contact: doppelsounds@example.com)"
USER_AGENT = "Mozilla/5.0 (Doppler/0.2; research tool)"

# --------------------------------------------------------------------------
VERBOSE = False
def log(m): print(m, flush=True)
def vlog(m):
    if VERBOSE: print(f"  . {m}", flush=True)

# --------------------------------------------------------------------------
# SAFE HTTP (never raises)
# --------------------------------------------------------------------------

def _http(url, headers=None, timeout=HTTP_TIMEOUT):
    headers = headers or {}
    headers.setdefault("User-Agent", USER_AGENT)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def safe_get_json(url, headers=None):
    last = ""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            raw = _http(url, headers=headers)
            try:
                return True, json.loads(raw), ""
            except json.JSONDecodeError as e:
                return False, None, f"bad JSON: {e}"
        except urllib.error.HTTPError as e:
            body = ""
            try: body = e.read().decode("utf-8", errors="replace")[:160]
            except Exception: pass
            last = f"HTTP {e.code} {e.reason} :: {body}"
            if e.code in (400, 401, 403):
                return False, None, last
        except urllib.error.URLError as e:
            last = f"URL error: {e.reason}"
        except Exception as e:  # noqa - never crash a run
            last = f"{type(e).__name__}: {e}"
        if attempt < HTTP_RETRIES:
            time.sleep(HTTP_BACKOFF * (2 ** (attempt - 1)))
    return False, None, last

# --------------------------------------------------------------------------
# MOCK FIXTURES (offline dry-run pipeline test)
# --------------------------------------------------------------------------

MOCK_AUTOCOMPLETE = {
    "martin garrix": ["martin garrix bizarre", "martin garrix animals",
                      "martin garrix ultra 2026", "martin garrix & sacha bizarre"],
    "martin garrix bizarre": ["martin garrix bizarre remake", "martin garrix bizarre flp"],
    "hardwell": ["hardwell turn up the bass", "hardwell spaceman", "hardwell 2026 id"],
    "hardwell turn up the bass": ["hardwell turn up the bass remake"],
    "anyma": ["anyma bad angel", "anyma explosion"],
    "anyma bad angel": ["anyma bad angel remix"],
    "john summit": ["john summit lights go out", "john summit shiver"],
    "john summit lights go out": ["john summit lights go out remake"],
    "alesso": ["alesso & sacha destiny", "alesso years"],
}
MOCK_YT = {
    "bizarre":          {"demand_views": 850000,  "newest_days": 9,   "remake_count": 2,  "remake_top": 3100},
    "turn up the bass": {"demand_views": 120000,  "newest_days": 15,  "remake_count": 1,  "remake_top": 900},
    "lights go out":    {"demand_views": 640000,  "newest_days": 6,   "remake_count": 0,  "remake_top": 0},
    "bad angel":        {"demand_views": 2200000, "newest_days": 4,   "remake_count": 14, "remake_top": 240000},
    "animals":          {"demand_views": 9000000, "newest_days": 200, "remake_count": 60, "remake_top": 800000},
    "years":            {"demand_views": 300000,  "newest_days": 120, "remake_count": 8,  "remake_top": 50000},
    "spaceman":         {"demand_views": 500000,  "newest_days": 90,  "remake_count": 5,  "remake_top": 40000},
    "explosion":        {"demand_views": 40000,   "newest_days": 30,  "remake_count": 0,  "remake_top": 0},
}
MOCK_MB_RELEASED = {"animals", "bad angel", "years", "spaceman", "shiver"}
MOCK_GENRE = {"brazilian phonk": 1800000, "phonk": 1200000, "hard techno": 300000,
              "future rave": 260000, "big room": 90000, "hardstyle": 700000,
              "afro house": 950000, "slap house": 140000, "melodic techno": 400000,
              "progressive house": 520000}

# --------------------------------------------------------------------------
# COLLECTORS
# --------------------------------------------------------------------------

def autocomplete(query, live):
    q = query.strip().lower()
    if not live:
        return list(MOCK_AUTOCOMPLETE.get(q, []))
    url = ("https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q="
           + urllib.parse.quote(q))
    ok, obj, err = safe_get_json(url)
    time.sleep(AUTOCOMPLETE_SLEEP)
    if not ok or not isinstance(obj, list) or len(obj) < 2:
        vlog(f"autocomplete miss '{q}': {err or 'shape'}")
        return []
    return [s for s in obj[1] if isinstance(s, str)]

def clean_track(artist, suggestion):
    """
    Suggestion -> clean track name, or None. Hardened for collab noise
    ('alesso & sacha destiny' -> 'destiny'): strip artist, take title side of a
    dash separator, then peel a leading collaborator clause.
    """
    a = artist.lower().strip()
    s = suggestion.lower().strip()
    if not s.startswith(a):
        return None
    rest = s[len(a):].strip(" -:–—")
    for sep in [" - ", " – ", " — ", ": "]:
        if sep in rest:
            rest = rest.split(sep)[-1].strip()
    toks = rest.split()
    while toks and toks[0] in COLLAB_LEADERS:
        toks = toks[1:]
        if toks:            # drop the collaborator's name after the connector
            toks = toks[1:]
    rest = " ".join(toks).strip()
    # strip trailing search-modifier noise ("forever lyrics" -> "forever")
    toks = rest.split()
    while toks and toks[-1] in TRAILING_NOISE:
        toks.pop()
    rest = " ".join(toks).strip()
    if not rest or len(rest) < 3 or rest in GENERIC_TAILS:
        return None
    return rest

def derive_candidates(artist, live):
    seeds = autocomplete(artist.lower(), live) + autocomplete(artist.lower() + " ", live)
    out, seen = [], set()
    for rank, s in enumerate(seeds):
        track = clean_track(artist, s)
        if not track or track in seen:
            continue
        seen.add(track)
        out.append({"artist": artist, "track": track, "demand_rank": rank})
    return out

def buyer_intent(artist, track, live):
    """Rank-sensitive buyer intent. Finds the TOP autocomplete position where a
    high-value term (remake/flp/instrumental/...) appears - ranking higher =
    stronger signal that searchers want exactly what you sell."""
    base = f"{artist.lower()} {track}"
    sugg = autocomplete(base + " ", live) or autocomplete(base, live)
    best_rank, matched = None, []
    for i, s in enumerate(sugg):
        sl = s.lower()
        for w in HIGH_INTENT:
            if w in sl:
                matched.append(w)
                if best_rank is None or i < best_rank:
                    best_rank = i
    low = any(w in " ".join(sugg).lower() for w in LOW_INTENT)
    return {"best_rank": best_rank, "matched": sorted(set(matched)), "low": low}

def buyer_intent_mult(bi):
    """1.0 (no intent) up to BI_MAX when a high-value term is the #1 suggestion;
    decays with rank. Returns (multiplier, short_label_for_report)."""
    if not bi:
        return 1.0, "—"
    r = bi.get("best_rank")
    if r is not None:
        strength = max(0.0, 1 - r / 8.0)           # #1 -> full; ~#8 -> none
        mult = BI_NEUTRAL + (BI_MAX - BI_NEUTRAL) * strength
        return mult, f"{'/'.join(bi['matched'][:2])} @#{r + 1}"
    if bi.get("low"):
        return BI_NEUTRAL + (BI_MAX - BI_NEUTRAL) * 0.2, "weak"
    return 1.0, "none"

def yt_signal(artist, track, api_key, budget, live, state):
    """ONE YouTube search -> demand (real videos) + competition (remake titles)."""
    key = track.lower()
    if not live:
        m = MOCK_YT.get(key)
        if not m:
            return {"status": "unknown", "reason": "no mock"}
        return {"status": "ok", **m}
    if not api_key:
        return {"status": "unknown", "reason": "no YT_API_KEY"}
    if state["yt_searches_used"] >= budget:
        return {"status": "unknown", "reason": "yt budget reached"}
    q = urllib.parse.quote(f"{artist} {track}")
    url = ("https://www.googleapis.com/youtube/v3/search?part=snippet&type=video"
           "&maxResults=25&order=relevance&q=" + q + "&key=" + urllib.parse.quote(api_key))
    ok, obj, err = safe_get_json(url)
    state["yt_searches_used"] += 1
    if not ok:
        if "403" in err: state["yt_quota_hit"] = True
        return {"status": "unknown", "reason": err}
    ids = [it["id"]["videoId"] for it in obj.get("items", []) if it.get("id", {}).get("videoId")]
    if not ids:
        return {"status": "ok", "demand_views": 0, "newest_days": 999, "remake_count": 0, "remake_top": 0}
    vurl = ("https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id="
            + ",".join(ids[:25]) + "&key=" + urllib.parse.quote(api_key))
    ok2, obj2, err2 = safe_get_json(vurl)
    if not ok2:
        return {"status": "partial", "reason": err2, "demand_views": 0,
                "newest_days": 999, "remake_count": 0, "remake_top": 0}
    now = dt.datetime.now(dt.timezone.utc)
    demand_views, newest_days, remake_count, remake_top = 0, 999, 0, 0
    for v in obj2.get("items", []):
        title = v.get("snippet", {}).get("title", "")
        try: views = int(v["statistics"].get("viewCount", 0))
        except (KeyError, ValueError): views = 0
        try:
            pub = v["snippet"]["publishedAt"].replace("Z", "+00:00")
            days = max((now - dt.datetime.fromisoformat(pub)).days, 0)
        except Exception:
            days = 999
        if REMAKE_MARKERS.search(title):
            remake_count += 1; remake_top = max(remake_top, views)
        else:
            demand_views = max(demand_views, views); newest_days = min(newest_days, days)
    return {"status": "ok", "demand_views": demand_views, "newest_days": newest_days,
            "remake_count": remake_count, "remake_top": remake_top}

def musicbrainz_released(artist, track, live):
    key = track.lower()
    if not live:
        return {"status": "ok", "released": key in MOCK_MB_RELEASED}
    q = urllib.parse.quote(f'artist:"{artist}" AND recording:"{track}"')
    url = f"https://musicbrainz.org/ws/2/recording/?query={q}&fmt=json&limit=3"
    ok, obj, err = safe_get_json(url, headers={"User-Agent": MB_USER_AGENT})
    time.sleep(MUSICBRAINZ_SLEEP)
    if not ok:
        return {"status": "unknown", "reason": err}
    recs = obj.get("recordings", [])
    hit = any(int(r.get("score", 0)) >= 90 for r in recs)
    return {"status": "ok", "released": hit}

def _iso_days_ago(days):
    d = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    return urllib.parse.quote(d.strftime("%Y-%m-%dT%H:%M:%SZ"))

def genre_pulse(genre, api_key, live, state):
    if not live:
        v = MOCK_GENRE.get(genre)
        return {"status": "ok", "activity": v} if v is not None else {"status": "unknown"}
    if not api_key or state["yt_searches_used"] >= state["yt_budget"]:
        return {"status": "unknown", "reason": "no key / budget"}
    q = urllib.parse.quote(f"{genre} mix 2026")
    url = ("https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&order=viewCount"
           "&publishedAfter=" + _iso_days_ago(60) + "&maxResults=10&q=" + q
           + "&key=" + urllib.parse.quote(api_key))
    ok, obj, err = safe_get_json(url)
    state["yt_searches_used"] += 1
    if not ok:
        return {"status": "unknown", "reason": err}
    ids = [it["id"]["videoId"] for it in obj.get("items", []) if it.get("id", {}).get("videoId")]
    if not ids:
        return {"status": "ok", "activity": 0}
    vurl = ("https://www.googleapis.com/youtube/v3/videos?part=statistics&id="
            + ",".join(ids) + "&key=" + urllib.parse.quote(api_key))
    ok2, obj2, _ = safe_get_json(vurl)
    if not ok2:
        return {"status": "ok", "activity": 0}
    views = sorted(int(v["statistics"].get("viewCount", 0)) for v in obj2.get("items", []) if v.get("statistics"))
    return {"status": "ok", "activity": views[len(views)//2] if views else 0}

# --------------------------------------------------------------------------
# SCORING (demand-first; gap + competition are multipliers)
# --------------------------------------------------------------------------

def clamp01(x): return max(0.0, min(1.0, x))

def log_scale(views, ceiling_log=7.0):
    if not views or views <= 0: return 0.0
    return clamp01(math.log10(views) / ceiling_log)

def score_candidate(snap, prev):
    comp = {}
    dv = snap.get("demand_views")
    if dv is None:
        comp["demand"] = None
    else:
        base = log_scale(dv)
        nd = snap.get("newest_days")
        recency = 0.1 * (1 - nd / 30.0) if (nd is not None and nd < 30) else 0.0
        comp["demand"] = clamp01(base + recency)
    if prev is None or prev.get("demand01") is None or comp["demand"] is None:
        comp["velocity"] = None
    else:
        comp["velocity"] = clamp01(0.5 + (comp["demand"] - prev["demand01"]) * 2.0)

    avail = {k: v for k, v in comp.items() if v is not None and k in WEIGHTS}
    wsum = sum(WEIGHTS[k] for k in avail) or 1.0
    base = sum(WEIGHTS[k] * avail[k] for k in avail) / wsum

    released = snap.get("released")
    gap_mult = GAP_NEUTRAL if released is None else (GAP_BOOST if released is False else GAP_RELEASED)
    rc = snap.get("remake_count")
    if rc is None:
        comp_mult = 1.0
    else:
        rt = snap.get("remake_top") or 0
        openness = 0.5 * clamp01(1 - rc / 15.0) + 0.5 * clamp01(1 - rt / 500000.0)
        comp_mult = COMP_MIN + (COMP_MAX - COMP_MIN) * openness

    bi_mult, bi_label = buyer_intent_mult(snap.get("bi"))
    # score is an uncapped opportunity INDEX (higher = better, not a percentage).
    # buyer intent is now the biggest lever, so strong-intent tracks can exceed 100.
    score = 100.0 * base * gap_mult * comp_mult * bi_mult
    snap["demand01"] = comp["demand"]
    snap["bi_label"] = bi_label
    return round(score, 1), comp, gap_mult, comp_mult, bi_mult

# --------------------------------------------------------------------------
# STORAGE
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  run_id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, mode TEXT,
  sources_ok TEXT, sources_failed TEXT, yt_used INTEGER);
CREATE TABLE IF NOT EXISTS candidates(
  id INTEGER PRIMARY KEY AUTOINCREMENT, artist TEXT, track TEXT,
  first_seen TEXT, last_seen TEXT, UNIQUE(artist, track));
CREATE TABLE IF NOT EXISTS snapshots(
  id INTEGER PRIMARY KEY AUTOINCREMENT, candidate_id INTEGER, run_id INTEGER,
  captured_at TEXT, demand01 REAL, demand_views INTEGER, newest_days INTEGER,
  buyer_intent_hits INTEGER, bi_rank INTEGER, released INTEGER, remake_count INTEGER,
  remake_top INTEGER, score REAL);
CREATE TABLE IF NOT EXISTS genres(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, genre TEXT,
  activity INTEGER, captured_at TEXT);
"""

def db_connect(path):
    con = sqlite3.connect(path); con.executescript(SCHEMA); _migrate(con); con.commit(); return con

# Columns v0.2 expects. If an older DB exists, CREATE TABLE IF NOT EXISTS won't
# alter it, so we add any missing columns here (forward migration, no data loss).
EXPECTED_COLUMNS = {
    "snapshots": {"candidate_id": "INTEGER", "run_id": "INTEGER", "captured_at": "TEXT",
                  "demand01": "REAL", "demand_views": "INTEGER", "newest_days": "INTEGER",
                  "buyer_intent_hits": "INTEGER", "bi_rank": "INTEGER", "released": "INTEGER",
                  "remake_count": "INTEGER", "remake_top": "INTEGER", "score": "REAL"},
    "runs": {"started_at": "TEXT", "mode": "TEXT", "sources_ok": "TEXT",
             "sources_failed": "TEXT", "yt_used": "INTEGER"},
    "candidates": {"artist": "TEXT", "track": "TEXT", "first_seen": "TEXT", "last_seen": "TEXT"},
    "genres": {"run_id": "INTEGER", "genre": "TEXT", "activity": "INTEGER", "captured_at": "TEXT"},
}

def _migrate(con):
    """Add any columns a previous version's tables are missing. Never raises."""
    for table, cols in EXPECTED_COLUMNS.items():
        try:
            existing = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
        except sqlite3.OperationalError:
            continue
        if not existing:
            continue  # table absent; SCHEMA already created the current version
        for col, typ in cols.items():
            if col not in existing:
                try:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
                    vlog(f"migrated {table}.{col}")
                except sqlite3.OperationalError as e:
                    vlog(f"migrate {table}.{col} failed: {e}")
    con.commit()
def db_new_run(con, mode):
    cur = con.execute("INSERT INTO runs(started_at, mode) VALUES(?,?)",
                      (dt.datetime.now().isoformat(timespec="seconds"), mode))
    con.commit(); return cur.lastrowid
def db_candidate(con, artist, track):
    now = dt.datetime.now().isoformat(timespec="seconds")
    con.execute("""INSERT INTO candidates(artist,track,first_seen,last_seen) VALUES(?,?,?,?)
                   ON CONFLICT(artist,track) DO UPDATE SET last_seen=excluded.last_seen""",
                (artist, track, now, now)); con.commit()
    return con.execute("SELECT id FROM candidates WHERE artist=? AND track=?", (artist, track)).fetchone()[0]
def db_prev(con, cid, run_id):
    r = con.execute("""SELECT demand01 FROM snapshots WHERE candidate_id=? AND run_id<?
                       ORDER BY run_id DESC LIMIT 1""", (cid, run_id)).fetchone()
    return {"demand01": r[0]} if r else None
def db_save(con, cid, run_id, s, score):
    con.execute("""INSERT INTO snapshots(candidate_id,run_id,captured_at,demand01,demand_views,
                   newest_days,buyer_intent_hits,bi_rank,released,remake_count,remake_top,score)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (cid, run_id, dt.datetime.now().isoformat(timespec="seconds"),
                 s.get("demand01"), s.get("demand_views"), s.get("newest_days"),
                 s.get("buyer_intent_hits"), s.get("bi_rank"),
                 None if s.get("released") is None else int(s["released"]),
                 s.get("remake_count"), s.get("remake_top"), score)); con.commit()

# --------------------------------------------------------------------------
# REPORT
# --------------------------------------------------------------------------

def yt_url(a, t): return "https://www.youtube.com/results?search_query=" + urllib.parse.quote(f"{a} {t}")
def gap_txt(s):
    r = s.get("released"); return "unknown" if r is None else ("UNRELEASED" if r is False else "released")
def comp_txt(s):
    return "unchecked" if s.get("remake_count") is None else f"{s['remake_count']} remakes / top {s.get('remake_top',0):,}"
def demand_txt(s):
    dv = s.get("demand_views")
    if dv is None: return "—"
    return f"{dv:,} views" + (f", {s['newest_days']}d" if s.get("newest_days", 999) < 400 else "")
def vel_txt(comp):
    v = comp.get("velocity")
    return "—" if v is None else ("rising" if v > 0.55 else ("falling" if v < 0.45 else "flat"))

def write_report(path, rows, genre_rows, meta):
    L = ["# Doppler — What to Make Next",
         f"*Run {meta['run_id']} · {meta['mode']} · {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}*\n"]
    if meta["mode"].startswith("DRY"):
        L.append("> ⚠️ **Dry-run on mock data.** Fixtures, not live numbers. Use `--go` for real signals.\n")
    L.append("Demand-first: a track only ranks if people are **searching** for it (demand is 70% of "
             "the base). Buyer intent — a search like *remake/flp/instrumental* ranking high in "
             "autocomplete — is now the biggest multiplier (up to 1.7×), so strong-intent tracks can "
             "score above 100. The number is a relative opportunity index (higher = better), not a "
             "percentage. Verify any row via its link.\n")
    L.append("| # | Track | Score | YouTube demand | Buyer intent | Gap | Competition | Velocity | Verify |")
    L.append("|---|-------|------:|----------------|--------------|-----|-------------|----------|--------|")
    for i, (s, score, comp) in enumerate(rows, 1):
        bi_txt = s.get("bi_label", "—")
        L.append(f"| {i} | {s['artist']} – {s['track']} | **{score}** | {demand_txt(s)} | {bi_txt} | "
                 f"{gap_txt(s)} | {comp_txt(s)} | {vel_txt(comp)} | [YT]({yt_url(s['artist'], s['track'])}) |")
    if genre_rows:
        L.append("\n## Rising genres — pack candidates")
        L.append("Where a genre is hot, remake several tracks in it and bundle a longer pack video.\n")
        L.append("| # | Genre | YouTube activity (median recent views) |")
        L.append("|---|-------|----------------------------------------|")
        for i, (g, act) in enumerate(genre_rows, 1):
            L.append(f"| {i} | {g} | {act:,} |")
    L.append("\n---\n### Run status")
    L.append(f"- Mode: **{meta['mode']}**")
    L.append(f"- Sources OK: {', '.join(meta['ok']) or 'none'}")
    if meta["failed"]:
        L.append(f"- Sources FAILED: {', '.join(meta['failed'])}")
    L.append(f"- Candidates scored: {len(rows)} · YouTube searches used: {meta['yt_used']}"
             + (" (quota hit)" if meta.get("yt_quota_hit") else ""))
    if rows and any(r[2].get("velocity") is None for r in rows):
        L.append("- Velocity: **pending** — needs a second run to measure acceleration.")
    L.append("\n*Tested offline: demand-first scoring, multipliers, parser, SQLite, report, graceful "
             "degradation. NOT yet verified against live API payloads — run `--go` and paste output so "
             "we harden the real-response parsing.*")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))

# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------

def main():
    global VERBOSE
    ap = argparse.ArgumentParser(description="Doppler v0.2 - Gap Scanner (demand-first)")
    ap.add_argument("--go", action="store_true", help="hit LIVE APIs (default: mock dry-run)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--genres", action="store_true", help="also run the genre-pulse module")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--max-yt-searches", type=int, default=60)
    ap.add_argument("--db", default="doppler.db")
    ap.add_argument("--out", default="doppler_report.md")
    a = ap.parse_args()
    VERBOSE = a.verbose
    live = a.go
    mode = "LIVE" if live else "DRY-RUN (mock data)"
    log(f"Doppler v0.2 — {mode}")
    yt_key = os.environ.get("YT_API_KEY", "")
    if live and not yt_key:
        log("  (no YT_API_KEY → demand+competition unchecked; still runs on autocomplete)")

    state = {"yt_searches_used": 0, "yt_quota_hit": False, "yt_budget": a.max_yt_searches}
    ok, failed = set(), set()
    con = db_connect(a.db)
    run_id = db_new_run(con, mode)

    cands = []
    for artist in WATCHLIST:
        try:
            f = derive_candidates(artist, live)
            if f: ok.add("autocomplete")
            cands.extend(f); vlog(f"{artist}: {len(f)}")
        except Exception as e:  # noqa
            failed.add("autocomplete"); vlog(f"{artist} failed: {e}")
    seen, uniq = set(), []
    for c in cands:
        k = (c["artist"].lower(), c["track"])
        if k not in seen: seen.add(k); uniq.append(c)
    cands = uniq[:a.top]
    if not cands and live: failed.add("autocomplete")
    log(f"  {len(cands)} unique candidates")

    rows = []
    for c in cands:
        s = dict(c)
        try:
            bi = buyer_intent(c["artist"], c["track"], live)
            s["bi"] = bi
            s["buyer_intent_hits"] = len(bi["matched"])
            s["bi_rank"] = bi["best_rank"]
        except Exception as e:  # noqa
            s["bi"] = None; s["buyer_intent_hits"] = None; s["bi_rank"] = None
            vlog(f"bi fail {c['track']}: {e}")
        y = yt_signal(c["artist"], c["track"], yt_key, a.max_yt_searches, live, state)
        if y.get("status") in ("ok", "partial"):
            if y["status"] == "ok": ok.add("youtube")
            s["demand_views"] = y.get("demand_views"); s["newest_days"] = y.get("newest_days")
            s["remake_count"] = y.get("remake_count"); s["remake_top"] = y.get("remake_top")
        else:
            if live and yt_key: failed.add("youtube")
            s["demand_views"] = None; s["remake_count"] = None
        mb = musicbrainz_released(c["artist"], c["track"], live)
        if mb.get("status") == "ok":
            ok.add("musicbrainz"); s["released"] = mb["released"]
        else:
            if live: failed.add("musicbrainz")
            s["released"] = None
        cid = db_candidate(con, c["artist"], c["track"])
        prev = db_prev(con, cid, run_id)
        score, comp, gm, cm, bim = score_candidate(s, prev)
        db_save(con, cid, run_id, s, score)
        rows.append((s, score, comp))
    rows.sort(key=lambda x: x[1], reverse=True)

    genre_rows = []
    if a.genres:
        for g in GENRE_WATCHLIST:
            r = genre_pulse(g, yt_key, live, state)
            if r.get("status") == "ok" and r.get("activity") is not None:
                ok.add("genre"); genre_rows.append((g, r["activity"]))
                con.execute("INSERT INTO genres(run_id,genre,activity,captured_at) VALUES(?,?,?,?)",
                            (run_id, g, r["activity"], dt.datetime.now().isoformat(timespec="seconds")))
        con.commit()
        genre_rows.sort(key=lambda x: x[1], reverse=True)

    con.execute("UPDATE runs SET sources_ok=?, sources_failed=?, yt_used=? WHERE run_id=?",
                (",".join(sorted(ok)), ",".join(sorted(failed)), state["yt_searches_used"], run_id))
    con.commit(); con.close()

    meta = {"run_id": run_id, "mode": mode, "ok": sorted(ok), "failed": sorted(failed),
            "yt_used": state["yt_searches_used"], "yt_quota_hit": state["yt_quota_hit"]}
    write_report(a.out, rows, genre_rows, meta)
    log(f"  wrote {a.out} ({len(rows)} ranked" + (f", {len(genre_rows)} genres" if genre_rows else "") + ")")
    log(f"  sources ok: {sorted(ok) or 'none'}" + (f" | FAILED: {sorted(failed)}" if failed else ""))
    if rows:
        log(f"  top pick: {rows[0][0]['artist']} – {rows[0][0]['track']} (score {rows[0][1]})")

if __name__ == "__main__":
    main()
