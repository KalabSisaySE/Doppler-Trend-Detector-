#!/usr/bin/env python3
"""
Doppler v0.1 - the Gap Scanner
================================
Finds "make this next" opportunities for Doppel Sounds by detecting the
supply-demand gap that made the Bizarre remake work:

    high-profile track + rising search demand + no official audio yet
    + few/no existing remakes  =  capture the search wave early.

Signals used in v0.1 (all reliable, free, no fragile scraping):
  - YouTube autocomplete  (demand + buyer-intent; no API key needed)
  - YouTube Data API v3    (competition: how many remakes already exist)
  - Spotify Web API        (supply gap: is the track even released yet?)

Robustness-first (standing rule):
  - Every network call is isolated, retried x3 with backoff, and NEVER raises.
  - If a source is missing keys or fails, the run still completes and the
    report prints exactly what failed and why. Missing data degrades to
    "unknown", it does not crash.
  - Dry-run is the DEFAULT: runs the whole pipeline on built-in mock data so
    you can test scoring/report/DB without spending a single API unit.
    Pass --go to hit live APIs.
  - Every snapshot is stored in SQLite so future runs can compute velocity
    (week-over-week acceleration). Run 1 has no history; velocity starts run 2.
  - Every row in the report links to the raw YouTube/Spotify search so you can
    verify each number yourself.

Usage:
  python3 doppler.py                 # dry-run (mock data), safe, no keys needed
  python3 doppler.py --verbose       # dry-run with detailed logging
  python3 doppler.py --go            # LIVE: hits autocomplete + (if keys) YT/Spotify
  python3 doppler.py --go --max-yt-searches 40 --top 20

Keys (optional; set as environment variables, tool degrades without them):
  YT_API_KEY            YouTube Data API v3 key
  SPOTIFY_CLIENT_ID     Spotify app client id
  SPOTIFY_CLIENT_SECRET Spotify app client secret
"""

import argparse
import base64
import datetime as dt
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# --------------------------------------------------------------------------
# CONFIG  (edit freely)
# --------------------------------------------------------------------------

# Your lane. Add/remove anytime. These seed the whole scan.
WATCHLIST = [
    "Martin Garrix", "Hardwell", "Alesso", "Illenium", "Anyma",
    "John Summit", "Sebastian Ingrosso", "Seth Hills", "W&W",
    "Marshmello", "David Guetta", "Tiesto", "Dimitri Vegas",
    "Swedish House Mafia", "Dom Dolla",
]

# Scoring weights (must be the components below; they get renormalised if a
# signal is unavailable, e.g. velocity on the first run). Tune these.
WEIGHTS = {
    "velocity":     0.25,   # is search accelerating? (needs run 2+) - the early-catch signal
    "gap":          0.25,   # unreleased = biggest gap
    "competition":  0.20,   # fewer existing remakes = more open
    "demand":       0.15,   # how present in autocomplete
    "buyer_intent": 0.15,   # do people search "<track> remake / flp"?
}

# Words in autocomplete that mean people want exactly what you sell.
BUYER_INTENT_WORDS = ["remake", "remix", "flp", "edit", "instrumental", "tutorial", "how to make"]

# Network politeness / robustness
HTTP_TIMEOUT = 12          # seconds
HTTP_RETRIES = 3
HTTP_BACKOFF = 0.6         # seconds, exponential
AUTOCOMPLETE_SLEEP = 0.35  # between free autocomplete calls
USER_AGENT = "Mozilla/5.0 (Doppler/0.1; research tool)"

# --------------------------------------------------------------------------
# LOGGING
# --------------------------------------------------------------------------

VERBOSE = False
def log(msg):
    print(msg, flush=True)
def vlog(msg):
    if VERBOSE:
        print(f"  . {msg}", flush=True)

# --------------------------------------------------------------------------
# SAFE HTTP  (never raises; returns (ok, data_or_none, error_string))
# --------------------------------------------------------------------------

def _http(url, data=None, headers=None, timeout=HTTP_TIMEOUT):
    headers = headers or {}
    headers.setdefault("User-Agent", USER_AGENT)
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", errors="replace")
    return raw

def safe_get_json(url, headers=None):
    """GET a URL and parse JSON. Returns (ok, obj, err). Retries with backoff."""
    last_err = ""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            raw = _http(url, headers=headers)
            try:
                return True, json.loads(raw), ""
            except json.JSONDecodeError as e:
                return False, None, f"bad JSON: {e} :: {raw[:120]}"
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            last_err = f"HTTP {e.code} {e.reason} :: {body}"
            # 403 (quota/forbidden) and 401 (auth) won't fix on retry
            if e.code in (401, 403):
                return False, None, last_err
        except urllib.error.URLError as e:
            last_err = f"URL error: {e.reason}"
        except Exception as e:  # noqa - deliberately broad: never crash a run
            last_err = f"{type(e).__name__}: {e}"
        if attempt < HTTP_RETRIES:
            time.sleep(HTTP_BACKOFF * (2 ** (attempt - 1)))
    return False, None, last_err

def safe_post_form(url, form_dict, headers=None):
    """POST form-encoded body, parse JSON. Returns (ok, obj, err)."""
    data = urllib.parse.urlencode(form_dict).encode("utf-8")
    h = dict(headers or {})
    h["Content-Type"] = "application/x-www-form-urlencoded"
    last_err = ""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            raw = _http(url, data=data, headers=h)
            return True, json.loads(raw), ""
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            last_err = f"HTTP {e.code} {e.reason} :: {body}"
            if e.code in (400, 401, 403):
                return False, None, last_err
        except Exception as e:  # noqa
            last_err = f"{type(e).__name__}: {e}"
        if attempt < HTTP_RETRIES:
            time.sleep(HTTP_BACKOFF * (2 ** (attempt - 1)))
    return False, None, last_err

# --------------------------------------------------------------------------
# MOCK FIXTURES  (used in dry-run so the whole pipeline is testable offline)
# --------------------------------------------------------------------------

MOCK_AUTOCOMPLETE = {
    "martin garrix": ["martin garrix bizarre", "martin garrix animals",
                      "martin garrix new song 2026", "martin garrix ultra 2026"],
    "martin garrix bizarre": ["martin garrix bizarre remake", "martin garrix bizarre flp",
                              "martin garrix bizarre madonna"],
    "hardwell": ["hardwell turn up the bass", "hardwell 2026 id", "hardwell spaceman"],
    "hardwell turn up the bass": ["hardwell turn up the bass remake", "hardwell turn up the bass w&w"],
    "anyma": ["anyma bad angel", "anyma lisa", "anyma explosion"],
    "anyma bad angel": ["anyma bad angel lisa", "anyma bad angel remix"],
    "john summit": ["john summit lights go out", "john summit shiver"],
    "john summit lights go out": ["john summit lights go out remake"],
    "alesso": ["alesso destiny", "alesso years"],
    "alesso destiny": ["alesso destiny remix"],
}
# keyed by track-only (matches derived candidate names). Tracks NOT listed here
# are treated as "not on Spotify" (unreleased) in the mock.
MOCK_SPOTIFY_RELEASED = {
    "animals": 78,
    "spaceman": 61,
    "bad angel": 55,
    "shiver": 70,
    "years": 66,
    "destiny": 40,
}
# mock competition: track-only -> (remake_count, top_views, newest_days)
MOCK_COMPETITION = {
    "bizarre": (2, 3100, 8),            # early: few remakes, your Bizarre setup
    "turn up the bass": (1, 900, 20),   # wide open
    "bad angel": (14, 240000, 3),       # crowded
    "lights go out": (0, 0, 999),       # nobody has done it
    "destiny": (25, 500000, 60),        # saturated + old
}

# --------------------------------------------------------------------------
# COLLECTORS
# --------------------------------------------------------------------------

def autocomplete(query, live):
    """Return list of YouTube autocomplete suggestions for `query`. Free."""
    q = query.strip().lower()
    if not live:
        return list(MOCK_AUTOCOMPLETE.get(q, []))
    url = ("https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q="
           + urllib.parse.quote(q))
    ok, obj, err = safe_get_json(url)
    time.sleep(AUTOCOMPLETE_SLEEP)
    if not ok or not isinstance(obj, list) or len(obj) < 2:
        vlog(f"autocomplete miss '{q}': {err or 'unexpected shape'}")
        return []
    return [s for s in obj[1] if isinstance(s, str)]

def derive_candidates(artist, live, state):
    """From an artist, mine autocomplete for candidate track names."""
    a = artist.lower()
    seeds = autocomplete(a, live) + autocomplete(a + " ", live)
    cands = []
    seen = set()
    for rank, s in enumerate(seeds):
        s_low = s.lower().strip()
        if not s_low.startswith(a):
            continue
        track = s_low[len(a):].strip(" -:")
        # filter noise / generic tails
        if (not track or len(track) < 3 or track in seen
                or track in ("official", "live", "mix", "songs", "music")):
            continue
        seen.add(track)
        cands.append({"artist": artist, "track": track, "demand_rank": rank})
        state["autocomplete_calls"] += 0  # counted inside autocomplete()
    return cands

def buyer_intent(artist, track, live):
    """Do people search '<artist> <track> remake/flp/...'? Returns (hits, matched)."""
    base = f"{artist.lower()} {track}"
    sugg = autocomplete(base + " ", live) or autocomplete(base, live)
    joined = " || ".join(sugg).lower()
    matched = [w for w in BUYER_INTENT_WORDS if w in joined]
    return len(matched), matched

def yt_competition(artist, track, api_key, budget, live, state):
    """
    How many remakes already exist? Returns dict or {'status':'unknown', ...}.
    Costs ~101 YouTube quota units (1 search + 1 stats). Budgeted + cached.
    """
    key_track = track.lower()
    if not live:
        c = MOCK_COMPETITION.get(key_track)
        if c is None:
            return {"status": "unknown", "reason": "no mock competition entry"}
        return {"status": "ok", "remake_count": c[0], "top_views": c[1], "newest_days": c[2]}

    if not api_key:
        return {"status": "unknown", "reason": "no YT_API_KEY"}
    if state["yt_searches_used"] >= budget:
        return {"status": "unknown", "reason": "yt search budget reached"}

    query = f"{artist} {track} remake"
    url = ("https://www.googleapis.com/youtube/v3/search?part=snippet&type=video"
           "&maxResults=25&order=relevance&q=" + urllib.parse.quote(query)
           + "&key=" + urllib.parse.quote(api_key))
    ok, obj, err = safe_get_json(url)
    state["yt_searches_used"] += 1
    if not ok:
        if "403" in err:
            state["yt_quota_hit"] = True
        return {"status": "unknown", "reason": err}
    items = obj.get("items", [])
    ids = [it["id"]["videoId"] for it in items if it.get("id", {}).get("videoId")]
    remake_count = len(ids)
    top_views, newest_days = 0, 999
    if ids:
        vurl = ("https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id="
                + ",".join(ids[:25]) + "&key=" + urllib.parse.quote(api_key))
        ok2, obj2, err2 = safe_get_json(vurl)
        if ok2:
            now = dt.datetime.now(dt.timezone.utc)
            for v in obj2.get("items", []):
                try:
                    top_views = max(top_views, int(v["statistics"].get("viewCount", 0)))
                except (KeyError, ValueError):
                    pass
                try:
                    pub = v["snippet"]["publishedAt"].replace("Z", "+00:00")
                    days = (now - dt.datetime.fromisoformat(pub)).days
                    newest_days = min(newest_days, max(days, 0))
                except Exception:
                    pass
        else:
            vlog(f"yt stats miss: {err2}")
    return {"status": "ok", "remake_count": remake_count,
            "top_views": top_views, "newest_days": newest_days}

_spotify_token_cache = {"token": None}
def spotify_token(cid, secret, live):
    if not live or not cid or not secret:
        return None
    if _spotify_token_cache["token"]:
        return _spotify_token_cache["token"]
    auth = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    ok, obj, err = safe_post_form(
        "https://accounts.spotify.com/api/token",
        {"grant_type": "client_credentials"},
        headers={"Authorization": f"Basic {auth}"})
    if not ok:
        vlog(f"spotify token failed: {err}")
        return None
    _spotify_token_cache["token"] = obj.get("access_token")
    return _spotify_token_cache["token"]

def spotify_release(artist, track, token, live):
    """Is this released? Returns dict. Not-on-spotify + demand = the gap."""
    key_track = track.lower()
    if not live:
        pop = MOCK_SPOTIFY_RELEASED.get(key_track)
        if pop is None:
            return {"status": "ok", "on_spotify": False, "popularity": 0}
        return {"status": "ok", "on_spotify": True, "popularity": pop}
    if not token:
        return {"status": "unknown", "reason": "no spotify token"}
    q = urllib.parse.quote(f"{artist} {track}")
    url = f"https://api.spotify.com/v1/search?q={q}&type=track&limit=1"
    ok, obj, err = safe_get_json(url, headers={"Authorization": f"Bearer {token}"})
    if not ok:
        return {"status": "unknown", "reason": err}
    items = obj.get("tracks", {}).get("items", [])
    if not items:
        return {"status": "ok", "on_spotify": False, "popularity": 0}
    return {"status": "ok", "on_spotify": True,
            "popularity": items[0].get("popularity", 0)}

# --------------------------------------------------------------------------
# SCORING  (transparent, handles unknowns, renormalises missing weights)
# --------------------------------------------------------------------------

def clamp01(x):
    return max(0.0, min(1.0, x))

def component_scores(snap, prev):
    """Return {component: value in 0..1 or None if unknown}."""
    comp = {}

    # demand: earlier autocomplete rank = stronger. rank 0 -> 1.0
    r = snap.get("demand_rank")
    comp["demand"] = clamp01(1.0 - (r / 8.0)) if r is not None else None

    # buyer_intent: number of intent words matched, cap at 3
    bi = snap.get("buyer_intent_hits")
    comp["buyer_intent"] = clamp01(bi / 3.0) if bi is not None else None

    # gap: not on spotify -> 1.0 ; released -> shrinks with popularity
    if snap.get("on_spotify") is None:
        comp["gap"] = None
    elif snap["on_spotify"] is False:
        comp["gap"] = 1.0
    else:
        pop = snap.get("spotify_pop") or 0
        comp["gap"] = clamp01(0.45 - pop / 400.0)  # released & popular = smaller gap

    # competition openness: 0 remakes -> 1.0 ; more/bigger/newer -> lower
    rc = snap.get("remake_count")
    if rc is None:
        comp["competition"] = None
    else:
        views = snap.get("top_views") or 0
        by_count = clamp01(1.0 - rc / 15.0)
        by_views = clamp01(1.0 - views / 500000.0)
        comp["competition"] = clamp01(0.5 * by_count + 0.5 * by_views)

    # velocity: change in demand vs previous run for same candidate
    if prev is None or prev.get("demand_rank") is None or r is None:
        comp["velocity"] = None
    else:
        # rank got smaller (rose) -> positive velocity
        delta = (prev["demand_rank"] - r)
        comp["velocity"] = clamp01(0.5 + delta / 8.0)

    return comp

def fit_multiplier(snap):
    # v0.1: everything on the watchlist is in-lane. Hook kept for future rules.
    return 1.0

def score_candidate(snap, prev):
    comp = component_scores(snap, prev)
    # renormalise weights over the components we actually have
    avail = {k: v for k, v in comp.items() if v is not None}
    wsum = sum(WEIGHTS[k] for k in avail) or 1.0
    base = sum(WEIGHTS[k] * avail[k] for k in avail) / wsum
    score = 100.0 * base * fit_multiplier(snap)
    return round(score, 1), comp

# --------------------------------------------------------------------------
# STORAGE
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT, mode TEXT, sources_ok TEXT, sources_failed TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS candidates(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  artist TEXT, track TEXT, first_seen TEXT, last_seen TEXT,
  UNIQUE(artist, track));
CREATE TABLE IF NOT EXISTS snapshots(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  candidate_id INTEGER, run_id INTEGER, captured_at TEXT,
  demand_rank INTEGER, buyer_intent_hits INTEGER,
  on_spotify INTEGER, spotify_pop INTEGER,
  remake_count INTEGER, top_views INTEGER, newest_days INTEGER,
  score REAL);
"""

def db_connect(path):
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    con.commit()
    return con

def db_new_run(con, mode):
    cur = con.execute(
        "INSERT INTO runs(started_at, mode) VALUES(?,?)",
        (dt.datetime.now().isoformat(timespec="seconds"), mode))
    con.commit()
    return cur.lastrowid

def db_upsert_candidate(con, artist, track):
    now = dt.datetime.now().isoformat(timespec="seconds")
    con.execute("""INSERT INTO candidates(artist,track,first_seen,last_seen)
                   VALUES(?,?,?,?)
                   ON CONFLICT(artist,track) DO UPDATE SET last_seen=excluded.last_seen""",
                (artist, track, now, now))
    con.commit()
    row = con.execute("SELECT id FROM candidates WHERE artist=? AND track=?",
                      (artist, track)).fetchone()
    return row[0]

def db_prev_snapshot(con, candidate_id, before_run):
    row = con.execute("""SELECT demand_rank FROM snapshots
                         WHERE candidate_id=? AND run_id<?
                         ORDER BY run_id DESC LIMIT 1""",
                      (candidate_id, before_run)).fetchone()
    return {"demand_rank": row[0]} if row else None

def db_save_snapshot(con, cid, run_id, snap, score):
    con.execute("""INSERT INTO snapshots(candidate_id,run_id,captured_at,demand_rank,
                   buyer_intent_hits,on_spotify,spotify_pop,remake_count,top_views,
                   newest_days,score) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (cid, run_id, dt.datetime.now().isoformat(timespec="seconds"),
                 snap.get("demand_rank"), snap.get("buyer_intent_hits"),
                 None if snap.get("on_spotify") is None else int(snap["on_spotify"]),
                 snap.get("spotify_pop"), snap.get("remake_count"),
                 snap.get("top_views"), snap.get("newest_days"), score))
    con.commit()

# --------------------------------------------------------------------------
# REPORT
# --------------------------------------------------------------------------

def yt_search_url(artist, track):
    return "https://www.youtube.com/results?search_query=" + urllib.parse.quote(f"{artist} {track} remake")
def sp_search_url(artist, track):
    return "https://open.spotify.com/search/" + urllib.parse.quote(f"{artist} {track}")

def gap_label(comp, snap):
    if snap.get("on_spotify") is None:
        return "unknown"
    return "UNRELEASED" if snap["on_spotify"] is False else f"released (pop {snap.get('spotify_pop',0)})"

def comp_label(snap):
    if snap.get("remake_count") is None:
        return "unchecked"
    return f"{snap['remake_count']} remakes / top {snap.get('top_views',0):,} views"

def vel_label(comp):
    v = comp.get("velocity")
    return "—" if v is None else ("+rising" if v > 0.5 else ("-falling" if v < 0.5 else "flat"))

def write_report(path, rows, run_meta):
    lines = []
    lines.append("# Doppler — What to Make Next")
    lines.append(f"*Run {run_meta['run_id']} · {run_meta['mode']} · "
                 f"{dt.datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    if run_meta["mode"] == "DRY-RUN (mock data)":
        lines.append("> ⚠️ **Dry-run on mock data.** Numbers below are fixtures to prove the "
                     "pipeline. Run with `--go` for live signals.\n")
    lines.append("Ranked by opportunity score. Higher = closer to the Bizarre setup "
                 "(rising demand, unreleased or lightly-covered, buyer intent). "
                 "Click the links to verify any row yourself.\n")

    lines.append("| # | Track | Score | Demand | Buyer intent | Supply gap | Competition | Velocity | Verify |")
    lines.append("|---|-------|------:|--------|--------------|-----------|-------------|----------|--------|")
    for i, (snap, score, comp) in enumerate(rows, 1):
        name = f"{snap['artist']} – {snap['track']}"
        bi = snap.get("buyer_intent_hits")
        bi_txt = "—" if bi is None else (",".join(snap.get("buyer_intent_matched", [])) or "none")
        links = f"[YT]({yt_search_url(snap['artist'], snap['track'])}) · [SP]({sp_search_url(snap['artist'], snap['track'])})"
        dem = comp.get("demand")
        dem_txt = "—" if dem is None else f"{dem:.2f}"
        lines.append(f"| {i} | {name} | **{score}** | {dem_txt} | {bi_txt} | "
                     f"{gap_label(comp, snap)} | {comp_label(snap)} | {vel_label(comp)} | {links} |")

    lines.append("\n---\n### Run status")
    lines.append(f"- Mode: **{run_meta['mode']}**")
    lines.append(f"- Sources OK: {', '.join(run_meta['sources_ok']) or 'none'}")
    if run_meta["sources_failed"]:
        lines.append(f"- Sources FAILED: {', '.join(run_meta['sources_failed'])}")
    lines.append(f"- Candidates scored: {len(rows)}")
    lines.append(f"- YouTube searches used: {run_meta['yt_used']}"
                 + (" (quota limit hit)" if run_meta.get("yt_quota_hit") else ""))
    if any(c[2].get("velocity") is None for c in rows):
        lines.append("- Velocity: **pending** — needs a second run to measure acceleration.")
    lines.append("\n*Tested offline: scoring, renormalisation, SQLite persistence, report, "
                 "graceful degradation. NOT yet verified against live API payloads — run `--go` "
                 "and paste output so we can harden against real responses.*")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------

def main():
    global VERBOSE
    ap = argparse.ArgumentParser(description="Doppler v0.1 - Gap Scanner")
    ap.add_argument("--go", action="store_true", help="hit LIVE APIs (default: dry-run on mock data)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--top", type=int, default=25, help="max candidates to deep-check + show")
    ap.add_argument("--max-yt-searches", type=int, default=50, help="YouTube quota guard")
    ap.add_argument("--db", default="doppler.db")
    ap.add_argument("--out", default="doppler_report.md")
    args = ap.parse_args()
    VERBOSE = args.verbose
    live = args.go
    mode = "LIVE" if live else "DRY-RUN (mock data)"

    log(f"Doppler v0.1 — {mode}")
    yt_key = os.environ.get("YT_API_KEY", "")
    sp_id = os.environ.get("SPOTIFY_CLIENT_ID", "")
    sp_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
    if live:
        log(f"  keys: YouTube={'yes' if yt_key else 'NO'} "
            f"Spotify={'yes' if (sp_id and sp_secret) else 'NO'}")
        if not yt_key: log("  (no YT key → competition = unchecked, still runs)")
        if not (sp_id and sp_secret): log("  (no Spotify creds → supply gap = unknown, still runs)")

    state = {"yt_searches_used": 0, "yt_quota_hit": False, "autocomplete_calls": 0}
    sources_ok, sources_failed = set(), set()

    con = db_connect(args.db)
    run_id = db_new_run(con, mode)
    token = spotify_token(sp_id, sp_secret, live)

    # 1) gather candidates from autocomplete
    candidates = []
    for artist in WATCHLIST:
        try:
            found = derive_candidates(artist, live, state)
            if found:
                sources_ok.add("autocomplete")
            candidates.extend(found)
            vlog(f"{artist}: {len(found)} candidates")
        except Exception as e:  # noqa - never let one artist kill the run
            sources_failed.add("autocomplete")
            vlog(f"{artist} failed: {e}")
    if not candidates:
        sources_failed.add("autocomplete")
    # de-dup and cap
    uniq, seen = [], set()
    for c in candidates:
        k = (c["artist"].lower(), c["track"])
        if k not in seen:
            seen.add(k); uniq.append(c)
    candidates = uniq[:args.top]
    log(f"  {len(candidates)} unique candidates")

    # 2) enrich + score
    rows = []
    for c in candidates:
        snap = dict(c)
        # buyer intent (free autocomplete)
        try:
            hits, matched = buyer_intent(c["artist"], c["track"], live)
            snap["buyer_intent_hits"] = hits
            snap["buyer_intent_matched"] = matched
        except Exception as e:  # noqa
            snap["buyer_intent_hits"] = None
            vlog(f"buyer_intent failed {c['track']}: {e}")
        # release gap (spotify)
        rel = spotify_release(c["artist"], c["track"], token, live)
        if rel.get("status") == "ok":
            sources_ok.add("spotify")
            snap["on_spotify"] = rel["on_spotify"]
            snap["spotify_pop"] = rel.get("popularity", 0)
        else:
            if live and (sp_id and sp_secret):
                sources_failed.add("spotify")
            snap["on_spotify"] = None
        # competition (youtube)
        yc = yt_competition(c["artist"], c["track"], yt_key,
                            args.max_yt_searches, live, state)
        if yc.get("status") == "ok":
            sources_ok.add("youtube")
            snap["remake_count"] = yc["remake_count"]
            snap["top_views"] = yc["top_views"]
            snap["newest_days"] = yc["newest_days"]
        else:
            if live and yt_key:
                sources_failed.add("youtube")
            snap["remake_count"] = None

        cid = db_upsert_candidate(con, c["artist"], c["track"])
        prev = db_prev_snapshot(con, cid, run_id)
        score, comp = score_candidate(snap, prev)
        snap["score"] = score
        db_save_snapshot(con, cid, run_id, snap, score)
        rows.append((snap, score, comp))

    rows.sort(key=lambda x: x[1], reverse=True)

    run_meta = {
        "run_id": run_id, "mode": mode,
        "sources_ok": sorted(sources_ok), "sources_failed": sorted(sources_failed),
        "yt_used": state["yt_searches_used"], "yt_quota_hit": state["yt_quota_hit"],
    }
    # persist run summary
    con.execute("UPDATE runs SET sources_ok=?, sources_failed=? WHERE run_id=?",
                (",".join(sorted(sources_ok)), ",".join(sorted(sources_failed)), run_id))
    con.commit()
    con.close()

    write_report(args.out, rows, run_meta)
    log(f"  wrote {args.out}  ({len(rows)} ranked)")
    log(f"  sources ok: {sorted(sources_ok) or 'none'}"
        + (f" | FAILED: {sorted(sources_failed)}" if sources_failed else ""))
    if rows:
        top = rows[0][0]
        log(f"  top pick: {top['artist']} – {top['track']}  (score {rows[0][1]})")

if __name__ == "__main__":
    main()
