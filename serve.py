#!/usr/bin/env python3
"""Serveur local pour la veille Finistère.

Sert le tableau de bord et expose une petite API REST permettant à l'onglet
Admin de gérer les flux et déclencher une mise à jour.

  GET  /                  → veille.html
  GET  /data.json         → données agrégées
  GET  /api/feeds         → liste des flux (avec statut du dernier fetch)
  POST /api/feeds         → ajouter un flux  {url,name,category,filter_finistere,enabled}
  PATCH /api/feeds        → modifier un flux {url, ...patch}
  DELETE /api/feeds       → supprimer un flux {url}
  POST /api/refresh       → relance l'agrégation (synchrone, ~30s max)
  POST /api/feeds/test    → valide une URL en la fetchant {url}

Usage : python3 serve.py [--port 8765]
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import veille
from feeds import (
    CATEGORY_LABELS,
    load_feeds, save_feeds, load_status,
)

ROOT = Path(__file__).parent
HTML_FILE = ROOT / "veille.html"
DATA_FILE = ROOT / "data.json"

# Verrou pour empêcher deux refresh simultanés
_refresh_lock = threading.Lock()
_refresh_state = {"running": False, "last_run": None, "error": None}


# --------------------------------------------------------------------------- #
#  Logique métier                                                             #
# --------------------------------------------------------------------------- #

ALLOWED_CATEGORIES = set(CATEGORY_LABELS.keys())


def normalize_feed(payload: dict) -> dict:
    """Valide et normalise un flux entrant."""
    url = (payload.get("url") or "").strip()
    name = (payload.get("name") or "").strip()
    category = (payload.get("category") or "autres").strip()

    if not url:
        raise ValueError("URL manquante")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("URL invalide (http/https requis)")
    if not name:
        # déduit un nom à partir de l'host
        name = parsed.netloc.replace("www.", "")
    if category not in ALLOWED_CATEGORIES:
        category = "autres"

    return {
        "url": url,
        "name": name,
        "category": category,
        "filter_finistere": bool(payload.get("filter_finistere", False)),
        "enabled": bool(payload.get("enabled", True)),
    }


def add_feed(payload: dict) -> dict:
    feed = normalize_feed(payload)
    feeds = load_feeds()
    if any(f["url"] == feed["url"] for f in feeds):
        raise ValueError("Ce flux est déjà dans la liste.")
    feeds.append(feed)
    save_feeds(feeds)
    return feed


def update_feed(payload: dict) -> dict:
    url = (payload.get("url") or "").strip()
    if not url:
        raise ValueError("URL manquante")
    feeds = load_feeds()
    for i, f in enumerate(feeds):
        if f["url"] == url:
            patched = {**f, **payload}
            feeds[i] = normalize_feed(patched)
            save_feeds(feeds)
            return feeds[i]
    raise ValueError("Flux introuvable")


def delete_feed(payload: dict) -> bool:
    url = (payload.get("url") or "").strip()
    feeds = load_feeds()
    new = [f for f in feeds if f["url"] != url]
    if len(new) == len(feeds):
        raise ValueError("Flux introuvable")
    save_feeds(new)
    return True


def list_feeds_with_status() -> list[dict]:
    """Retourne la liste de flux enrichie du statut de la dernière collecte."""
    status = load_status()
    out = []
    for f in load_feeds():
        st = status.get(f["url"], {})
        out.append({
            **f,
            "status": {
                "ok": st.get("ok", None),
                "items_count": st.get("items_count"),
                "raw_count": st.get("raw_count"),
                "duration_s": st.get("duration_s"),
                "error": st.get("error"),
                "fetched_at": st.get("fetched_at"),
            },
        })
    return out


def test_feed(payload: dict) -> dict:
    """Valide une URL : la fetch sans l'enregistrer, retourne titre+nb d'articles."""
    url = (payload.get("url") or "").strip()
    if not url:
        raise ValueError("URL manquante")
    items, status = veille.fetch_feed(
        {"url": url, "name": url, "category": "autres", "filter_finistere": False},
        quiet=True,
    )
    return {
        "ok": status["ok"],
        "items_count": status["items_count"],
        "raw_count": status["raw_count"],
        "error": status["error"],
        "duration_s": status["duration_s"],
        "sample_titles": [it["title"] for it in items[:3]],
    }


def run_refresh() -> dict:
    """Recollecte tous les flux et régénère data.json + veille.html."""
    with _refresh_lock:
        _refresh_state["running"] = True
        _refresh_state["error"] = None
        try:
            items, _status = veille.fetch_all(quiet=True)
            DATA_FILE.write_text(
                json.dumps(items, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            HTML_FILE.write_text(
                veille.build_html(items, n_sources=len(load_feeds())),
                encoding="utf-8",
            )
            from datetime import datetime, timezone
            _refresh_state["last_run"] = datetime.now(timezone.utc).isoformat()
            return {"ok": True, "items": len(items),
                    "last_run": _refresh_state["last_run"]}
        except Exception as e:
            _refresh_state["error"] = str(e)
            raise
        finally:
            _refresh_state["running"] = False


# --------------------------------------------------------------------------- #
#  HTTP                                                                       #
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    server_version = "veille/1.0"

    # ----- helpers ----------------------------------------------------------
    def _send_json(self, code: int, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, ctype: str):
        if not path.exists():
            self.send_error(404, "Not found")
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except json.JSONDecodeError:
            raise ValueError("JSON invalide")

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    # ----- routes -----------------------------------------------------------
    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html", "/veille.html"):
            self._send_file(HTML_FILE, "text/html; charset=utf-8")
        elif path == "/data.json":
            self._send_file(DATA_FILE, "application/json; charset=utf-8")
        elif path == "/api/feeds":
            self._send_json(200, {
                "feeds": list_feeds_with_status(),
                "categories": CATEGORY_LABELS,
                "refresh": _refresh_state,
            })
        elif path == "/api/categories":
            self._send_json(200, CATEGORY_LABELS)
        else:
            self.send_error(404, "Not found")

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/feeds":
                payload = self._read_json()
                feed = add_feed(payload)
                self._send_json(201, {"ok": True, "feed": feed})
            elif path == "/api/feeds/test":
                payload = self._read_json()
                self._send_json(200, test_feed(payload))
            elif path == "/api/refresh":
                if _refresh_state["running"]:
                    self._send_json(409, {"ok": False, "error": "Mise à jour déjà en cours"})
                    return
                result = run_refresh()
                self._send_json(200, result)
            else:
                self.send_error(404, "Not found")
        except ValueError as e:
            self._send_json(400, {"ok": False, "error": str(e)})
        except Exception as e:
            self._send_json(500, {"ok": False, "error": str(e)})

    def do_PATCH(self):
        if urlparse(self.path).path != "/api/feeds":
            self.send_error(404, "Not found")
            return
        try:
            payload = self._read_json()
            feed = update_feed(payload)
            self._send_json(200, {"ok": True, "feed": feed})
        except ValueError as e:
            self._send_json(400, {"ok": False, "error": str(e)})

    def do_DELETE(self):
        if urlparse(self.path).path != "/api/feeds":
            self.send_error(404, "Not found")
            return
        try:
            payload = self._read_json()
            delete_feed(payload)
            self._send_json(200, {"ok": True})
        except ValueError as e:
            self._send_json(400, {"ok": False, "error": str(e)})


def main() -> int:
    ap = argparse.ArgumentParser(description="Serveur local de la veille Finistère")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-open", action="store_true", help="ne pas ouvrir le navigateur")
    args = ap.parse_args()

    if not HTML_FILE.exists():
        print("veille.html absent — première génération…", file=sys.stderr)
        run_refresh()

    addr = ("127.0.0.1", args.port)
    httpd = ThreadingHTTPServer(addr, Handler)
    url = f"http://{addr[0]}:{addr[1]}/"
    print(f"\n  Veille Finistère — {url}\n  Ctrl-C pour arrêter\n", file=sys.stderr)
    if not args.no_open:
        webbrowser.open(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nArrêt.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
