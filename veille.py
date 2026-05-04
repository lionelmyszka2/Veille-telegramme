#!/usr/bin/env python3
"""Veille Finistère — agrège les flux RSS définis dans feeds.py et génère
un tableau de bord HTML autonome (veille.html) ouvrable dans le navigateur.

Usage:
    python3 veille.py            # Met à jour veille.html et data.json
    python3 veille.py --quiet    # Sans logs
"""
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import html
import io
import json
import os
import re
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Corrige les erreurs SSL CERTIFICATE_VERIFY_FAILED courantes sur macOS Python
try:
    import certifi
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except ImportError:
    pass

import feedparser

from feeds import (
    FINISTERE_KEYWORDS, CATEGORY_LABELS, CITIES,
    load_feeds, save_status,
)

ROOT = Path(__file__).parent
HTML_OUT = ROOT / "veille.html"
JSON_OUT = ROOT / "data.json"

# UA volontairement minimal : certaines protections anti-bot
# (ex. lepeuplebreton.bzh) renvoient 403 sur les UA Chrome complets.
USER_AGENT = "Mozilla/5.0 (compatible; veille-finistere/1.0)"
USER_AGENT_FALLBACK = "Mozilla/5.0"
FETCH_TIMEOUT = 20           # secondes
MAX_WORKERS = 8
MAX_ITEMS_PER_FEED = 200     # garde les N plus récents par flux
MAX_RETRIES = 2              # tentatives par flux en cas d'échec transitoire


def log(msg: str, quiet: bool = False) -> None:
    if not quiet:
        print(msg, file=sys.stderr)


def strip_html(text: str) -> str:
    """Retire balises HTML et entités, normalise les espaces."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def matches_finistere(title: str, summary: str) -> bool:
    haystack = (title + " " + summary).lower()
    return any(kw in haystack for kw in FINISTERE_KEYWORDS)


def parse_date(entry) -> str | None:
    """Retourne une date ISO 8601 UTC, ou None."""
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        t = entry.get(key)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
            except (TypeError, ValueError):
                continue
    return None


def item_id(link: str, title: str) -> str:
    base = (link or "") + "|" + (title or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def fetch_feed(feed_def: dict, quiet: bool = False) -> tuple[list[dict], dict]:
    """Retourne (items, status). status décrit ce qui s'est passé pour ce flux."""
    url = feed_def["url"]
    name = feed_def["name"]
    category = feed_def["category"]
    filter_fr = feed_def.get("filter_finistere", False)

    status = {
        "url": url,
        "name": name,
        "ok": False,
        "items_count": 0,
        "raw_count": 0,
        "duration_s": 0.0,
        "error": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    t0 = time.time()
    parsed = None
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        ua = USER_AGENT_FALLBACK if attempt > 1 else USER_AGENT
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": ua,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
                "Accept-Encoding": "gzip, identity;q=0.5",
            })
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
            parsed = feedparser.parse(io.BytesIO(raw))
            if parsed.entries:
                break
            last_err = getattr(parsed, "bozo_exception", "aucun article")
        except Exception as e:
            last_err = e
        time.sleep(1.0 * attempt)

    status["duration_s"] = round(time.time() - t0, 2)

    if parsed is None or not parsed.entries:
        status["error"] = str(last_err)
        log(f"  [WARN] {name}: {last_err}", quiet)
        return [], status

    items = []
    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = strip_html(entry.get("title", ""))
        link = entry.get("link", "")
        summary = strip_html(entry.get("summary", "") or entry.get("description", ""))
        if len(summary) > 600:
            summary = summary[:600].rsplit(" ", 1)[0] + "…"

        if filter_fr and not matches_finistere(title, summary):
            continue
        if not (title and link):
            continue

        items.append({
            "id": item_id(link, title),
            "title": title,
            "link": link,
            "summary": summary,
            "date": parse_date(entry),
            "source": name,
            "category": category,
        })

    status["ok"] = True
    status["items_count"] = len(items)
    status["raw_count"] = len(parsed.entries)
    log(f"  [OK]  {name}: {len(items)} articles ({status['duration_s']}s)", quiet)
    return items, status


def fetch_all(quiet: bool = False) -> tuple[list[dict], dict]:
    feeds = [f for f in load_feeds() if f.get("enabled", True)]
    log(f"Récupération de {len(feeds)} flux…", quiet)
    all_items: list[dict] = []
    seen_ids: set[str] = set()
    status_map: dict[str, dict] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_feed, fd, quiet): fd for fd in feeds}
        for fut in concurrent.futures.as_completed(futures):
            items, st = fut.result()
            status_map[st["url"]] = st
            for item in items:
                if item["id"] in seen_ids:
                    continue
                seen_ids.add(item["id"])
                all_items.append(item)

    all_items.sort(key=lambda x: (x["date"] or ""), reverse=True)
    log(f"\nTotal : {len(all_items)} articles uniques.", quiet)
    save_status(status_map)
    return all_items, status_map


# --------------------------------------------------------------------------- #
#  HTML dashboard                                                             #
# --------------------------------------------------------------------------- #

HTML_TEMPLATE = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Veille Finistère</title>
<style>
/* ─── Design system — minimaliste, monochromatique ───────────────────── */
:root {
  --bg: #fafaf9;
  --panel: #ffffff;
  --ink: #0a0a0a;
  --muted: #737373;
  --faint: #a3a3a3;
  --line: #ededed;
  --hover: #f5f5f5;
  --accent: #0a0a0a;
  --accent-fg: #fafafa;
  --hi: #fef3c7;
  --shadow: 0 1px 2px rgba(0,0,0,.04);
  --radius: 10px;
  --radius-lg: 14px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0a0a0a;
    --panel: #131313;
    --ink: #fafafa;
    --muted: #a3a3a3;
    --faint: #525252;
    --line: #1f1f1f;
    --hover: #1a1a1a;
    --accent: #fafafa;
    --accent-fg: #0a0a0a;
    --hi: #4a3a0a;
    --shadow: 0 1px 2px rgba(0,0,0,.2);
  }
}

* { box-sizing: border-box; }
html, body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI",
        Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  font-feature-settings: "ss01", "cv11";
}
a { color: inherit; }
button { font: inherit; }

/* ─── Header (compact) ───────────────────────────────────────────────── */
header {
  position: sticky; top: 0; z-index: 10;
  background: rgba(250,250,249,.9);
  -webkit-backdrop-filter: saturate(140%) blur(10px);
          backdrop-filter: saturate(140%) blur(10px);
  border-bottom: 1px solid var(--line);
}
@media (prefers-color-scheme: dark) {
  header { background: rgba(10,10,10,.85); }
}
.header-inner { max-width: 1280px; margin: 0 auto; padding: 6px 24px 4px; }

.topbar {
  display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
  min-height: 28px;
}
.brand {
  display: flex; align-items: center; gap: 14px;
  font-size: 14px; font-weight: 600; letter-spacing: -.01em;
}
.brand-name::before {
  content: ""; display: inline-block; width: 6px; height: 6px;
  background: var(--ink); border-radius: 50%; margin-right: 8px;
  vertical-align: middle;
}
.topbar-meta {
  margin-left: auto; display: flex; align-items: center; gap: 12px;
  font-size: 12px; color: var(--muted);
}
.topbar-meta a { color: inherit; text-decoration: none; }
.topbar-meta a:hover { color: var(--ink); }

/* ─── Onglets compacts ───────────────────────────────────────────────── */
.tabs { display: flex; gap: 0; }
.tab {
  cursor: pointer; border: 0; background: transparent; color: var(--muted);
  padding: 4px 0; margin-right: 14px;
  font-size: 13px; font-weight: 500;
  border-bottom: 2px solid transparent;
  transition: color .15s;
}
.tab:hover { color: var(--ink); }
.tab.on { color: var(--ink); border-bottom-color: var(--ink); }

.view { display: none; }
.view.on { display: block; }

/* ─── Barre de filtres compacte ──────────────────────────────────────── */
.toolbar {
  display: flex; gap: 6px; align-items: center; flex-wrap: wrap;
  padding: 4px 0 2px;
}
.search {
  display: flex; align-items: center; gap: 8px;
  background: var(--panel); border: 1px solid var(--line);
  border-radius: 8px; padding: 4px 10px;
  flex: 1 1 260px; min-width: 200px;
  transition: border-color .15s;
}
.search:focus-within { border-color: var(--ink); }
.search svg { color: var(--faint); flex-shrink: 0; }
.search input {
  flex: 1; border: 0; outline: 0; background: transparent;
  color: inherit; font-size: 13px;
}
.search kbd {
  font-family: inherit; font-size: 10px; color: var(--faint);
  border: 1px solid var(--line); border-radius: 4px; padding: 1px 5px;
}

.btn {
  cursor: pointer; border: 1px solid var(--line); background: var(--panel);
  color: var(--ink); border-radius: 8px;
  padding: 4px 10px; font-size: 12px; font-weight: 500;
  transition: all .15s;
  white-space: nowrap;
}
.btn:hover { border-color: var(--ink); }
.btn.active { background: var(--accent); color: var(--accent-fg); border-color: var(--accent); }
.btn-primary { background: var(--accent); color: var(--accent-fg); border-color: var(--accent); }
.btn-primary:hover { opacity: .88; }
.btn-primary:disabled { opacity: .5; cursor: wait; }
.btn-ghost { background: transparent; border-color: transparent; color: var(--muted); }
.btn-ghost:hover { background: var(--hover); color: var(--ink); border-color: transparent; }

select.select {
  cursor: pointer;
  border: 1px solid var(--line); background: var(--panel);
  color: var(--ink); border-radius: var(--radius);
  padding: 8px 32px 8px 14px; font-size: 13px; font-weight: 500;
  appearance: none;
  background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6' fill='none'><path d='M1 1L5 5L9 1' stroke='%23737373' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/></svg>");
  background-repeat: no-repeat;
  background-position: right 12px center;
  transition: border-color .15s;
}
select.select:hover { border-color: var(--ink); }
select.select:focus { outline: 0; border-color: var(--ink); }

/* ─── Chips compactes ────────────────────────────────────────────────── */
.filters {
  display: flex; gap: 6px; flex-wrap: wrap; align-items: center;
  padding: 2px 0;
}
.chip-group { display: flex; gap: 3px; flex-wrap: wrap; align-items: center; }
.chip {
  cursor: pointer; user-select: none;
  font-size: 11px; font-weight: 500;
  padding: 2px 9px; border-radius: 999px;
  border: 1px solid var(--line); background: var(--panel);
  color: var(--muted);
  transition: all .12s;
  line-height: 1.55;
}
.chip:hover { color: var(--ink); border-color: var(--muted); }
.chip.on { background: var(--accent); color: var(--accent-fg); border-color: var(--accent); }
.chip .n {
  opacity: .55; margin-left: 4px; font-variant-numeric: tabular-nums;
  font-size: 10.5px;
}
.divider {
  width: 1px; height: 14px; background: var(--line); margin: 0 4px;
  flex-shrink: 0;
}
/* Rangée villes : wrap naturel pour ne pas cacher d'info */
.cities-scroll {
  display: flex; gap: 3px; align-items: center;
  flex-wrap: wrap;
  flex: 1 1 100%;
}
.chip-empty {
  font-size: 11.5px; color: var(--faint); font-style: italic;
  padding: 3px 0;
}

details.sources { font-size: 12px; }
details.sources-row { padding: 0 0 4px; }
details.sources summary {
  cursor: pointer; color: var(--faint); font-size: 11px;
  list-style: none;
  display: inline-block;
  padding: 2px 0;
}
details.sources summary::-webkit-details-marker { display: none; }
details.sources summary::after { content: " ▾"; }
details.sources[open] summary::after { content: " ▴"; }
.sources-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 4px 16px; margin-top: 8px; padding-bottom: 6px;
}
.sources-grid label {
  display: flex; align-items: center; gap: 6px;
  font-size: 11.5px; color: var(--muted); cursor: pointer;
}

/* ─── Layout principal ───────────────────────────────────────────────── */
main { max-width: 1280px; margin: 0 auto; padding: 12px 24px 60px; }

/* ─── Grille de tuiles ───────────────────────────────────────────────── */
.grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}
@media (max-width: 1024px) { .grid { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 640px)  { .grid { grid-template-columns: 1fr; } }

.tile {
  position: relative;
  display: flex; flex-direction: column;
  min-height: 280px;
  padding: 22px;
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  transition: border-color .15s, transform .15s, box-shadow .15s;
  cursor: pointer;
}
.tile:hover {
  border-color: var(--ink);
  box-shadow: var(--shadow);
}
.tile.read { opacity: .55; }
.tile.fav::before {
  content: ""; position: absolute; top: 14px; left: 0;
  width: 3px; height: 32px; background: var(--ink); border-radius: 0 2px 2px 0;
}

.tile-meta {
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  font-size: 11px; color: var(--muted);
  margin-bottom: 14px; min-height: 18px;
}
.tile-cat {
  font-size: 11px; font-weight: 500;
  padding: 2px 8px; border-radius: 999px;
  background: var(--hover); color: var(--muted);
}
.tile-city {
  font-size: 11px; font-weight: 500; color: var(--ink);
}
.tile-time { color: var(--faint); }
.tile-time::before { content: "·"; margin-right: 6px; color: var(--faint); }

.tile h2 {
  margin: 0 0 10px;
  font-size: 16px; font-weight: 600; letter-spacing: -.01em;
  line-height: 1.4;
  display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical;
  overflow: hidden;
}
.tile h2 a { text-decoration: none; }
.tile-summary {
  margin: 0; color: var(--muted); font-size: 13.5px; line-height: 1.55;
  display: -webkit-box; -webkit-line-clamp: 4; -webkit-box-orient: vertical;
  overflow: hidden;
  flex: 1;
}

.tile-footer {
  display: flex; justify-content: space-between; align-items: center;
  margin-top: 16px; padding-top: 14px;
  border-top: 1px solid var(--line);
  font-size: 12px;
}
.tile-source { color: var(--muted); }
.tile-actions { display: flex; gap: 4px; }

mark { background: var(--hi); color: inherit; padding: 0 2px; border-radius: 2px; }

.icon-btn {
  cursor: pointer; border: 0; background: transparent;
  color: var(--faint); font-size: 16px;
  padding: 4px 6px; border-radius: 6px;
  line-height: 1;
  transition: all .15s;
}
.icon-btn:hover { color: var(--ink); background: var(--hover); }
.icon-btn.on { color: var(--ink); }

.empty {
  grid-column: 1 / -1;
  text-align: center; padding: 80px 20px;
  color: var(--muted); font-size: 14px;
}

footer {
  text-align: center; color: var(--faint); font-size: 12px;
  padding: 60px 20px 40px;
}
footer code {
  background: var(--hover); padding: 2px 6px; border-radius: 4px;
  font-size: 11px;
}

/* ─── Admin ──────────────────────────────────────────────────────────── */
.admin-bar {
  display: flex; justify-content: space-between; align-items: center;
  margin: 0 0 24px; flex-wrap: wrap; gap: 12px;
}
.admin-bar h2 { margin: 0; font-size: 20px; font-weight: 600; letter-spacing: -.01em; }
.refresh-status { font-size: 13px; color: var(--muted); }
.refresh-status.err { color: #dc2626; }

.admin-card {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: var(--radius-lg);
  padding: 24px;
  margin-bottom: 18px;
}
.admin-card h3 {
  margin: 0 0 18px; font-size: 15px; font-weight: 600;
  letter-spacing: -.005em;
}
.field {
  display: grid; grid-template-columns: 160px 1fr; gap: 12px;
  align-items: center; margin-bottom: 12px;
}
.field label { font-size: 13px; color: var(--muted); }
.field input[type=text], .field input[type=url], .field select {
  width: 100%; padding: 9px 12px;
  border: 1px solid var(--line); border-radius: var(--radius);
  background: var(--bg); color: inherit; font-size: 14px;
  transition: border-color .15s;
}
.field input[type=text]:focus, .field input[type=url]:focus, .field select:focus {
  outline: 0; border-color: var(--ink);
}
.field input[type=checkbox] { justify-self: start; cursor: pointer; }
.form-actions { display: flex; gap: 8px; margin-top: 18px; }
.test-result {
  margin-top: 14px; padding: 12px 14px;
  border-radius: var(--radius); font-size: 13px;
  background: var(--hover); border: 1px solid var(--line);
}
.test-result.ok { border-color: #16a34a; background: #f0fdf4; color: #14532d; }
.test-result.err { border-color: #dc2626; background: #fef2f2; color: #7f1d1d; }
@media (prefers-color-scheme: dark) {
  .test-result.ok { background: #052e16; color: #86efac; border-color: #166534; }
  .test-result.err { background: #2a0a0a; color: #fca5a5; border-color: #7f1d1d; }
}

table.feeds { width: 100%; border-collapse: collapse; font-size: 13px; }
table.feeds th, table.feeds td {
  padding: 10px 12px; border-bottom: 1px solid var(--line);
  text-align: left; vertical-align: middle;
}
table.feeds th {
  font-weight: 500; color: var(--muted); font-size: 11px;
  text-transform: uppercase; letter-spacing: .06em;
}
table.feeds tr.disabled { opacity: .45; }
table.feeds td.url {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11px; color: var(--faint);
  max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.pill {
  display: inline-block; font-size: 11px; font-weight: 500;
  padding: 2px 8px; border-radius: 999px;
}
.pill.ok { background: #f0fdf4; color: #166534; }
.pill.err { background: #fef2f2; color: #991b1b; }
.pill.idle { background: var(--hover); color: var(--muted); }
@media (prefers-color-scheme: dark) {
  .pill.ok { background: #052e16; color: #86efac; }
  .pill.err { background: #2a0a0a; color: #fca5a5; }
}
.row-actions { display: flex; gap: 2px; justify-content: flex-end; }
.danger { color: #dc2626; }
.danger:hover { background: #fef2f2 !important; }
@media (prefers-color-scheme: dark) {
  .danger:hover { background: #2a0a0a !important; }
}

.toast {
  position: fixed; bottom: 24px; left: 50%; transform: translateX(-50%) translateY(8px);
  background: var(--ink); color: var(--bg);
  padding: 10px 16px; border-radius: var(--radius);
  font-size: 13px; font-weight: 500;
  opacity: 0; transition: all .2s; pointer-events: none; z-index: 50;
  box-shadow: 0 4px 24px rgba(0,0,0,.15);
}
.toast.on { opacity: 1; transform: translateX(-50%) translateY(0); }
.toast.err { background: #dc2626; color: #fff; }

.no-api {
  padding: 48px 24px; text-align: center; color: var(--muted);
  background: var(--panel); border: 1px solid var(--line);
  border-radius: var(--radius-lg);
}
.no-api code {
  background: var(--hover); padding: 4px 10px; border-radius: 6px;
  font-size: 13px; color: var(--ink);
}

.spinner {
  display: inline-block; width: 12px; height: 12px;
  border: 1.5px solid var(--line); border-top-color: var(--ink);
  border-radius: 50%; animation: spin .7s linear infinite;
  vertical-align: middle; margin-right: 8px;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<header>
  <div class="header-inner">
    <div class="topbar">
      <div class="brand">
        <span class="brand-name">Veille Finistère</span>
        <nav class="tabs">
          <button class="tab on" data-view="articles">Articles</button>
          <button class="tab" data-view="admin">Admin</button>
        </nav>
      </div>
      <div class="topbar-meta">
        <span id="count"></span>
        <a href="#" id="markall" title="Tout marquer comme lu">✓ tout</a>
        <span id="lastupdate"></span>
      </div>
    </div>
    <div class="view view-articles on">
      <div class="toolbar">
        <div class="search">
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
            <path d="M11 11L14 14M12.5 7.25C12.5 10.1495 10.1495 12.5 7.25 12.5C4.35051 12.5 2 10.1495 2 7.25C2 4.35051 4.35051 2 7.25 2C10.1495 2 12.5 4.35051 12.5 7.25Z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
          </svg>
          <input id="q" type="search" placeholder="Rechercher…" autofocus>
          <kbd>/</kbd>
        </div>
        <button class="btn btn-ghost" id="unread-only">Non lus</button>
        <button class="btn btn-ghost" id="fav-only">Favoris</button>
        <button class="btn btn-ghost" id="reset">Reset</button>
      </div>
      <div class="filters">
        <div class="chip-group" id="cats"></div>
        <div class="divider"></div>
        <div class="chip-group" id="daterange">
          <span class="chip" data-range="1">24h</span>
          <span class="chip" data-range="3">3j</span>
          <span class="chip on" data-range="7">7j</span>
          <span class="chip" data-range="30">30j</span>
          <span class="chip" data-range="0">∞</span>
        </div>
      </div>
      <div class="filters">
        <div class="cities-scroll" id="cities"></div>
      </div>
      <details class="sources sources-row" id="sources-detail">
        <summary>Sources <span id="srccount"></span></summary>
        <div class="sources-grid" id="srclist"></div>
      </details>
    </div>
  </div>
</header>
<main>
  <div class="view view-articles on">
    <div id="list" class="grid"></div>
  </div>

  <div class="view view-admin">
    <div id="admin-no-api" class="no-api" style="display:none">
      <p><strong>Lecture seule</strong></p>
      <p>Pour gérer les flux RSS (ajout, suppression, mise à jour à la demande),
      lance le serveur local :</p>
      <p><code>cd ~/Veille-telegramme &amp;&amp; python3 serve.py</code></p>
      <p style="margin-top:14px">Ou consulte la liste statique des flux configurés ci-dessous :</p>
      <div id="admin-readonly-list"></div>
    </div>

    <div id="admin-app" style="display:none">
      <div class="admin-bar">
        <h2>Gestion des flux</h2>
        <div style="display:flex;gap:10px;align-items:center">
          <span class="refresh-status" id="refresh-status"></span>
          <button class="btn btn-primary" id="btn-refresh">↻ Mettre à jour les articles</button>
        </div>
      </div>

      <div class="admin-card">
        <h3>Ajouter un flux RSS</h3>
        <div class="field">
          <label for="add-url">URL du flux *</label>
          <input id="add-url" type="url" placeholder="https://example.com/feed/" required>
        </div>
        <div class="field">
          <label for="add-name">Nom (facultatif)</label>
          <input id="add-name" type="text" placeholder="Déduit du domaine si vide">
        </div>
        <div class="field">
          <label for="add-cat">Catégorie</label>
          <select id="add-cat"></select>
        </div>
        <div class="field">
          <label for="add-filter">Filtrer Finistère</label>
          <input id="add-filter" type="checkbox">
        </div>
        <div class="form-actions">
          <button class="btn" id="btn-test">Tester l'URL</button>
          <button class="btn btn-primary" id="btn-add">Ajouter</button>
        </div>
        <div id="test-result" class="test-result" style="display:none"></div>
      </div>

      <div class="admin-card">
        <h3>Flux configurés <span id="feeds-count" style="color:var(--muted);font-weight:400"></span></h3>
        <div style="overflow-x:auto">
          <table class="feeds">
            <thead>
              <tr>
                <th>Nom</th>
                <th>Catégorie</th>
                <th>Finistère</th>
                <th>Activé</th>
                <th style="text-align:right">Articles</th>
                <th>Statut</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="feeds-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <footer>
    Outil de veille local · {{N_SOURCES}} sources · généré par <code>veille.py</code>
  </footer>
</main>
<div class="toast" id="toast"></div>

<script>
const ITEMS = {{DATA}};
const CATS = {{CATS}};
const CITIES = {{CITIES}};
const GENERATED = "{{GENERATED}}";

// Détecte les villes mentionnées dans chaque article (une fois au chargement)
ITEMS.forEach(it => {
  const hay = " " + (it.title + " " + it.summary).toLowerCase() + " ";
  it.cities = [];
  for (const [city, kws] of Object.entries(CITIES)) {
    if (kws.some(k => hay.includes(k))) it.cities.push(city);
  }
});

const STATE = {
  q: "",
  cats: new Set(Object.keys(CATS)),
  sources: null,            // null = toutes
  city: "",                 // "" = toutes
  rangeDays: 7,
  unreadOnly: false,
  favOnly: false,
};

// localStorage : articles lus + favoris (par id)
const READ = new Set(JSON.parse(localStorage.getItem("veille.read") || "[]"));
const FAV  = new Set(JSON.parse(localStorage.getItem("veille.fav")  || "[]"));
const saveRead = () => localStorage.setItem("veille.read", JSON.stringify([...READ]));
const saveFav  = () => localStorage.setItem("veille.fav",  JSON.stringify([...FAV]));

// dernière mise à jour (format compact)
{
  const d = new Date(GENERATED);
  const today = new Date();
  const sameDay = d.toDateString() === today.toDateString();
  document.getElementById("lastupdate").textContent =
    "maj " + (sameDay
      ? d.toLocaleTimeString("fr-FR", {hour:"2-digit", minute:"2-digit"})
      : d.toLocaleDateString("fr-FR", {day:"2-digit", month:"short"})
        + " " + d.toLocaleTimeString("fr-FR", {hour:"2-digit", minute:"2-digit"}));
}

// catégories : génère les chips
const sourceList = [...new Set(ITEMS.map(i => i.source))].sort();
const catCounts = {};
ITEMS.forEach(i => { catCounts[i.category] = (catCounts[i.category]||0)+1 });

const catsEl = document.getElementById("cats");
Object.entries(CATS).forEach(([k, label]) => {
  const c = document.createElement("span");
  c.className = "chip on";
  c.dataset.cat = k;
  c.innerHTML = label + ' <span class="n">' + (catCounts[k]||0) + '</span>';
  c.onclick = () => {
    if (STATE.cats.has(k)) { STATE.cats.delete(k); c.classList.remove("on"); }
    else { STATE.cats.add(k); c.classList.add("on"); }
    render();
  };
  catsEl.appendChild(c);
});

// liste des sources
const srclist = document.getElementById("srclist");
sourceList.forEach(s => {
  const lab = document.createElement("label");
  const cb  = document.createElement("input");
  cb.type = "checkbox"; cb.checked = true; cb.dataset.src = s;
  cb.onchange = () => {
    const checked = [...srclist.querySelectorAll("input")].filter(x=>x.checked).map(x=>x.dataset.src);
    STATE.sources = (checked.length === sourceList.length) ? null : new Set(checked);
    render();
  };
  lab.append(cb, document.createTextNode(" " + s));
  srclist.appendChild(lab);
});
document.getElementById("srccount").textContent = "(" + sourceList.length + ")";

// daterange
document.querySelectorAll("#daterange .chip").forEach(el => {
  el.onclick = () => {
    document.querySelectorAll("#daterange .chip").forEach(x=>x.classList.remove("on"));
    el.classList.add("on");
    STATE.rangeDays = parseInt(el.dataset.range, 10);
    render();
  };
});

// chips villes (peuplés dynamiquement par refreshFacets)
const citiesEl = document.getElementById("cities");
// Délégation : un seul listener pour tous les chips villes
citiesEl.addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (!chip) return;
  const v = chip.dataset.city || "";
  STATE.city = (v === STATE.city) ? "" : v;  // re-clic = désélection
  render();
});

// recherche
const qEl = document.getElementById("q");
qEl.oninput = () => { STATE.q = qEl.value.trim().toLowerCase(); render(); };
document.addEventListener("keydown", e => {
  if (e.key === "/" && document.activeElement !== qEl) { e.preventDefault(); qEl.focus(); }
});

// boutons
document.getElementById("unread-only").onclick = function() {
  STATE.unreadOnly = !STATE.unreadOnly;
  this.classList.toggle("active", STATE.unreadOnly); render();
};
document.getElementById("fav-only").onclick = function() {
  STATE.favOnly = !STATE.favOnly;
  this.classList.toggle("active", STATE.favOnly); render();
};
document.getElementById("reset").onclick = () => {
  qEl.value = ""; STATE.q = "";
  STATE.cats = new Set(Object.keys(CATS));
  catsEl.querySelectorAll(".chip").forEach(c => c.classList.add("on"));
  STATE.sources = null;
  srclist.querySelectorAll("input").forEach(c => c.checked = true);
  STATE.city = "";
  STATE.rangeDays = 7;
  document.querySelectorAll("#daterange .chip").forEach(x=>x.classList.toggle("on", x.dataset.range==="7"));
  STATE.unreadOnly = false; STATE.favOnly = false;
  document.getElementById("unread-only").classList.remove("active");
  document.getElementById("fav-only").classList.remove("active");
  render();
};
document.getElementById("markall").onclick = (e) => {
  e.preventDefault();
  filteredItems().forEach(i => READ.add(i.id));
  saveRead(); render();
};

// Applique tous les filtres SAUF celui passé en `skip` ('cat'|'city'|'source'|'date'|null)
// Permet aux compteurs de chaque facette d'ignorer son propre filtre.
function filterItems(skip) {
  const now = Date.now();
  const cutoff = STATE.rangeDays > 0 ? now - STATE.rangeDays*86400000 : 0;
  const tokens = STATE.q.split(/\s+/).filter(Boolean);
  return ITEMS.filter(i => {
    if (skip !== 'cat' && !STATE.cats.has(i.category)) return false;
    if (skip !== 'source' && STATE.sources && !STATE.sources.has(i.source)) return false;
    if (skip !== 'city' && STATE.city && !i.cities.includes(STATE.city)) return false;
    if (skip !== 'date') {
      if (cutoff && i.date) {
        if (new Date(i.date).getTime() < cutoff) return false;
      } else if (cutoff && !i.date) {
        return false;
      }
    }
    if (STATE.unreadOnly && READ.has(i.id)) return false;
    if (STATE.favOnly && !FAV.has(i.id)) return false;
    if (tokens.length) {
      const hay = (i.title + " " + i.summary).toLowerCase();
      for (const t of tokens) if (!hay.includes(t)) return false;
    }
    return true;
  });
}

const filteredItems = () => filterItems(null);

// Recalcule les compteurs des facettes en fonction des filtres actifs
function refreshFacets() {
  // ── Villes : compte par ville en ignorant le filtre ville (faceting)
  const itemsForCity = filterItems('city');
  const cityCounts = {};
  itemsForCity.forEach(i => i.cities.forEach(c => cityCounts[c] = (cityCounts[c]||0)+1));

  // Conserve la ville sélectionnée même si elle est désormais à 0
  const current = STATE.city;
  const keys = new Set(Object.keys(cityCounts).filter(k => cityCounts[k] > 0));
  if (current) keys.add(current);
  const sorted = [...keys].sort((a, b) => (cityCounts[b]||0) - (cityCounts[a]||0));

  const chips = ['<span class="chip ' + (current ? '' : 'on')
    + '" data-city="">Toutes <span class="n">'
    + sorted.reduce((s, c) => s + (cityCounts[c]||0), 0) + '</span></span>'];
  for (const c of sorted) {
    const n = cityCounts[c] || 0;
    const on = c === current ? ' on' : '';
    chips.push('<span class="chip' + on + '" data-city="' + escapeHtml(c) + '">'
      + escapeHtml(c) + ' <span class="n">' + n + '</span></span>');
  }
  if (sorted.length === 0) {
    citiesEl.innerHTML = '<span class="chip-empty">Aucune ville mentionnée dans les articles filtrés</span>';
  } else {
    citiesEl.innerHTML = chips.join("");
  }

  // ── Catégories : compte en ignorant le filtre catégorie
  const itemsForCat = filterItems('cat');
  const catCounts = {};
  itemsForCat.forEach(i => catCounts[i.category] = (catCounts[i.category]||0)+1);
  catsEl.querySelectorAll(".chip").forEach(chip => {
    const cat = chip.dataset.cat;
    const n = catCounts[cat] || 0;
    const nEl = chip.querySelector(".n");
    if (nEl) nEl.textContent = n;
    // griser si 0 articles dispos sous les autres filtres
    chip.style.opacity = (n === 0 && !STATE.cats.has(cat)) ? .4 : 1;
  });
}

function highlight(text, tokens) {
  if (!tokens.length) return text;
  const safe = tokens.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const re = new RegExp("(" + safe.join("|") + ")", "gi");
  return text.replace(re, "<mark>$1</mark>");
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso), now = new Date();
  const diff = (now - d) / 1000;
  if (diff < 3600) return Math.max(1, Math.floor(diff/60)) + " min";
  if (diff < 86400) return Math.floor(diff/3600) + " h";
  if (diff < 7*86400) return Math.floor(diff/86400) + " j";
  return d.toLocaleDateString("fr-FR", {day:"2-digit", month:"short", year:"numeric"});
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

function render() {
  refreshFacets();
  const items = filteredItems();
  const tokens = STATE.q.split(/\s+/).filter(Boolean);
  const list = document.getElementById("list");
  document.getElementById("count").textContent =
    items.length + " / " + ITEMS.length;

  if (!items.length) {
    list.innerHTML = '<div class="empty">Aucun article ne correspond aux filtres.</div>';
    return;
  }

  list.innerHTML = items.map(i => {
    const titleH = highlight(escapeHtml(i.title), tokens);
    const summH  = highlight(escapeHtml(i.summary), tokens);
    const isRead = READ.has(i.id), isFav = FAV.has(i.id);
    const cityH = i.cities.length ? '<span class="tile-city">' + escapeHtml(i.cities[0]) + '</span>' : '';
    return `
      <article class="tile ${isRead?'read':''} ${isFav?'fav':''}" data-id="${i.id}" data-link="${escapeHtml(i.link)}">
        <div class="tile-meta">
          <span class="tile-cat">${escapeHtml(CATS[i.category]||i.category)}</span>
          ${cityH}
          <span class="tile-time" title="${i.date||''}">${fmtDate(i.date)}</span>
        </div>
        <h2><a href="${escapeHtml(i.link)}" target="_blank" rel="noopener noreferrer">${titleH}</a></h2>
        ${i.summary ? '<p class="tile-summary">'+summH+'</p>' : '<p class="tile-summary" style="opacity:.4">—</p>'}
        <div class="tile-footer">
          <span class="tile-source">${escapeHtml(i.source)}</span>
          <div class="tile-actions">
            <button class="icon-btn ${isFav?'on':''}" data-act="fav" title="Favori">${isFav?'★':'☆'}</button>
            <button class="icon-btn ${isRead?'on':''}" data-act="read" title="Marquer lu">${isRead?'✓':'○'}</button>
          </div>
        </div>
      </article>
    `;
  }).join("");

  // listeners boutons fav/lu — stopPropagation pour ne pas déclencher le clic tuile
  list.querySelectorAll(".icon-btn").forEach(btn => {
    btn.onclick = (e) => {
      e.stopPropagation(); e.preventDefault();
      const tile = btn.closest(".tile");
      const id = tile.dataset.id;
      if (btn.dataset.act === "fav") {
        if (FAV.has(id)) FAV.delete(id); else FAV.add(id);
        saveFav();
      } else {
        if (READ.has(id)) READ.delete(id); else READ.add(id);
        saveRead();
      }
      render();
    };
  });

  // Clic n'importe où sur la tuile : ouvre l'article + marque lu
  list.querySelectorAll(".tile").forEach(tile => {
    tile.addEventListener("click", (e) => {
      // si le clic est sur le lien dans h2, le browser gère
      if (e.target.closest("a") || e.target.closest(".icon-btn")) return;
      const link = tile.dataset.link;
      const id = tile.dataset.id;
      READ.add(id); saveRead();
      window.open(link, "_blank", "noopener");
    });
  });

  // Clic direct sur le titre : marque lu (avant que le lien navigue)
  list.querySelectorAll(".tile h2 a").forEach(a => {
    a.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = a.closest(".tile").dataset.id;
      READ.add(id); saveRead();
    });
  });
}

render();

// ─────────────────────────────────────────────────────────────────────────
//  Navigation onglets + Admin
// ─────────────────────────────────────────────────────────────────────────

const tabs = document.querySelectorAll(".tab");
tabs.forEach(t => t.onclick = () => switchView(t.dataset.view));

function switchView(view) {
  tabs.forEach(t => t.classList.toggle("on", t.dataset.view === view));
  document.querySelectorAll(".view").forEach(v => {
    v.classList.toggle("on", v.classList.contains("view-" + view));
  });
  if (view === "admin" && !ADMIN.loaded) {
    initAdmin();
  }
}

const ADMIN = { hasApi: null, loaded: false, feeds: [], cats: CATS };

function showToast(msg, isErr) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.classList.toggle("err", !!isErr);
  el.classList.add("on");
  clearTimeout(showToast._t);
  showToast._t = setTimeout(() => el.classList.remove("on"), 3000);
}

async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  let json = null;
  try { json = await r.json(); } catch (_) {}
  if (!r.ok) {
    const msg = (json && json.error) || ("HTTP " + r.status);
    throw new Error(msg);
  }
  return json;
}

async function detectApi() {
  if (location.protocol === "file:") return false;
  try {
    const r = await fetch("/api/feeds", { method: "GET" });
    return r.ok;
  } catch (e) {
    return false;
  }
}

async function initAdmin() {
  ADMIN.loaded = true;
  ADMIN.hasApi = await detectApi();

  if (!ADMIN.hasApi) {
    document.getElementById("admin-no-api").style.display = "";
    document.getElementById("admin-app").style.display = "none";
    // Liste lecture-seule des sources connues à partir des articles
    const div = document.getElementById("admin-readonly-list");
    const bySource = {};
    ITEMS.forEach(i => {
      bySource[i.source] = bySource[i.source] || { name: i.source, category: i.category, count: 0 };
      bySource[i.source].count++;
    });
    const rows = Object.values(bySource).sort((a,b)=>b.count-a.count).map(s =>
      `<tr><td>${escapeHtml(s.name)}</td>
           <td><span class="tag cat-${s.category}">${escapeHtml(CATS[s.category]||s.category)}</span></td>
           <td style="text-align:right">${s.count}</td></tr>`
    ).join("");
    div.innerHTML = '<table class="feeds" style="margin-top:14px">'
      + '<thead><tr><th>Nom</th><th>Catégorie</th><th style="text-align:right">Articles</th></tr></thead>'
      + '<tbody>' + rows + '</tbody></table>';
    return;
  }

  document.getElementById("admin-no-api").style.display = "none";
  document.getElementById("admin-app").style.display = "";

  // alimente le select des catégories
  const sel = document.getElementById("add-cat");
  sel.innerHTML = Object.entries(CATS)
    .map(([k, v]) => `<option value="${k}">${escapeHtml(v)}</option>`).join("");

  document.getElementById("btn-refresh").onclick = doRefresh;
  document.getElementById("btn-test").onclick = doTest;
  document.getElementById("btn-add").onclick = doAdd;

  await loadFeeds();
}

async function loadFeeds() {
  try {
    const data = await api("GET", "/api/feeds");
    ADMIN.feeds = data.feeds;
    renderFeedsTable();
  } catch (e) {
    showToast("Erreur de chargement : " + e.message, true);
  }
}

function fmtAgo(iso) {
  if (!iso) return "—";
  const d = new Date(iso); const diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return "à l'instant";
  if (diff < 3600) return Math.floor(diff/60) + " min";
  if (diff < 86400) return Math.floor(diff/3600) + " h";
  return Math.floor(diff/86400) + " j";
}

function renderFeedsTable() {
  document.getElementById("feeds-count").textContent =
    "(" + ADMIN.feeds.length + ")";
  const tbody = document.getElementById("feeds-tbody");
  tbody.innerHTML = ADMIN.feeds.map(f => {
    const st = f.status || {};
    let statusHtml;
    if (st.ok === true) {
      statusHtml = `<span class="pill ok">OK</span>
        <span style="color:var(--muted);font-size:11px;margin-left:4px">${fmtAgo(st.fetched_at)}</span>`;
    } else if (st.ok === false) {
      const err = st.error ? escapeHtml(String(st.error).slice(0, 80)) : "erreur";
      statusHtml = `<span class="pill err" title="${err}">⚠ erreur</span>`;
    } else {
      statusHtml = `<span class="pill idle">jamais</span>`;
    }
    const safeUrl = escapeHtml(f.url);
    return `
      <tr class="${f.enabled ? '' : 'disabled'}" data-url="${safeUrl}">
        <td>
          <div style="font-weight:500">${escapeHtml(f.name)}</div>
          <div class="url" title="${safeUrl}">${safeUrl}</div>
        </td>
        <td>
          <select class="cat-sel" data-url="${safeUrl}" style="font-size:12px;padding:3px 6px;background:var(--bg);color:inherit;border:1px solid var(--line);border-radius:6px">
            ${Object.entries(ADMIN.cats).map(([k,v]) =>
              `<option value="${k}" ${k===f.category?'selected':''}>${escapeHtml(v)}</option>`
            ).join("")}
          </select>
        </td>
        <td><input type="checkbox" class="filter-cb" data-url="${safeUrl}" ${f.filter_finistere?'checked':''}></td>
        <td><input type="checkbox" class="enabled-cb" data-url="${safeUrl}" ${f.enabled?'checked':''}></td>
        <td style="text-align:right;font-variant-numeric:tabular-nums">${st.items_count ?? '—'}</td>
        <td>${statusHtml}</td>
        <td class="row-actions">
          <a class="icon-btn" href="${safeUrl}" target="_blank" rel="noopener" title="Ouvrir le flux">↗</a>
          <button class="icon-btn danger" data-act="delete" data-url="${safeUrl}" title="Supprimer">✕</button>
        </td>
      </tr>
    `;
  }).join("");

  tbody.querySelectorAll(".enabled-cb").forEach(cb => cb.onchange = () =>
    patchFeed(cb.dataset.url, { enabled: cb.checked }));
  tbody.querySelectorAll(".filter-cb").forEach(cb => cb.onchange = () =>
    patchFeed(cb.dataset.url, { filter_finistere: cb.checked }));
  tbody.querySelectorAll(".cat-sel").forEach(sel => sel.onchange = () =>
    patchFeed(sel.dataset.url, { category: sel.value }));
  tbody.querySelectorAll('[data-act="delete"]').forEach(b => b.onclick = () =>
    deleteFeed(b.dataset.url));
}

async function patchFeed(url, patch) {
  try {
    await api("PATCH", "/api/feeds", { url, ...patch });
    await loadFeeds();
    showToast("Mis à jour");
  } catch (e) {
    showToast(e.message, true);
    await loadFeeds();
  }
}

async function deleteFeed(url) {
  const f = ADMIN.feeds.find(x => x.url === url);
  if (!confirm("Supprimer le flux « " + (f?.name || url) + " » ?")) return;
  try {
    await api("DELETE", "/api/feeds", { url });
    await loadFeeds();
    showToast("Flux supprimé");
  } catch (e) {
    showToast(e.message, true);
  }
}

async function doTest() {
  const url = document.getElementById("add-url").value.trim();
  const out = document.getElementById("test-result");
  if (!url) { out.style.display = "none"; return; }
  out.style.display = "";
  out.className = "test-result";
  out.innerHTML = '<span class="spinner"></span> Test en cours…';
  try {
    const r = await api("POST", "/api/feeds/test", { url });
    if (r.ok) {
      out.className = "test-result ok";
      const titles = r.sample_titles.length
        ? '<ul style="margin:6px 0 0;padding-left:18px">'
          + r.sample_titles.map(t => '<li>' + escapeHtml(t) + '</li>').join("")
          + '</ul>'
        : '';
      out.innerHTML = '✓ <strong>' + r.raw_count + ' articles trouvés</strong> ('
        + r.duration_s + 's). Aperçu :' + titles;
      // pré-remplit le nom si vide
      const nameEl = document.getElementById("add-name");
      if (!nameEl.value.trim() && r.sample_titles[0]) {
        // suggère le host
        try { nameEl.value = new URL(url).hostname.replace(/^www\./, ""); } catch (_) {}
      }
    } else {
      out.className = "test-result err";
      out.textContent = "⚠ Aucun article récupéré : " + (r.error || "flux invalide");
    }
  } catch (e) {
    out.className = "test-result err";
    out.textContent = "Erreur : " + e.message;
  }
}

async function doAdd() {
  const payload = {
    url: document.getElementById("add-url").value.trim(),
    name: document.getElementById("add-name").value.trim(),
    category: document.getElementById("add-cat").value,
    filter_finistere: document.getElementById("add-filter").checked,
    enabled: true,
  };
  if (!payload.url) { showToast("URL requise", true); return; }
  try {
    await api("POST", "/api/feeds", payload);
    document.getElementById("add-url").value = "";
    document.getElementById("add-name").value = "";
    document.getElementById("add-filter").checked = false;
    document.getElementById("test-result").style.display = "none";
    await loadFeeds();
    showToast("Flux ajouté. Lance « Mettre à jour » pour récupérer ses articles.");
  } catch (e) {
    showToast(e.message, true);
  }
}

async function doRefresh() {
  const btn = document.getElementById("btn-refresh");
  const status = document.getElementById("refresh-status");
  btn.disabled = true;
  status.classList.remove("err");
  status.innerHTML = '<span class="spinner"></span> Mise à jour en cours…';
  try {
    const r = await api("POST", "/api/refresh");
    status.textContent = r.items + " articles récupérés. Recharge la page.";
    showToast("Mise à jour terminée — " + r.items + " articles");
    await loadFeeds();
    // Recharge la page pour rafraîchir la vue articles
    setTimeout(() => location.reload(), 1200);
  } catch (e) {
    status.textContent = "Erreur : " + e.message;
    status.classList.add("err");
    showToast(e.message, true);
  } finally {
    btn.disabled = false;
  }
}

// Détection silencieuse de l'API au chargement (active automatiquement
// l'onglet admin si le serveur est lancé).
detectApi().then(has => { ADMIN.hasApi = has; });
</script>
</body>
</html>
"""


def build_html(items: list[dict], n_sources: int) -> str:
    data_json = json.dumps(items, ensure_ascii=False)
    cats_json = json.dumps(CATEGORY_LABELS, ensure_ascii=False)
    cities_json = json.dumps(CITIES, ensure_ascii=False)
    generated = datetime.now(timezone.utc).isoformat()
    return (
        HTML_TEMPLATE
        .replace("{{DATA}}", data_json)
        .replace("{{CATS}}", cats_json)
        .replace("{{CITIES}}", cities_json)
        .replace("{{GENERATED}}", generated)
        .replace("{{N_SOURCES}}", str(n_sources))
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Veille Finistère — agrégation RSS")
    ap.add_argument("--quiet", action="store_true", help="silencieux")
    args = ap.parse_args()

    items, _status = fetch_all(quiet=args.quiet)

    JSON_OUT.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_OUT.write_text(build_html(items, n_sources=len(load_feeds())), encoding="utf-8")

    log(f"\n→ {HTML_OUT.relative_to(ROOT)}", quiet=args.quiet)
    log(f"→ {JSON_OUT.relative_to(ROOT)}", quiet=args.quiet)
    log(f"\nOuvre veille.html dans ton navigateur ou lance ./serve.sh pour l'admin.",
        quiet=args.quiet)
    return 0


if __name__ == "__main__":
    sys.exit(main())
