"""Configuration de la veille Finistère.

La liste des flux vit dans `feeds.json` (lue/écrite par le serveur admin).
Ce module ne contient plus que les constantes (mots-clés, catégories) et les
helpers de chargement / sauvegarde.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

ROOT = Path(__file__).parent
FEEDS_FILE = ROOT / "feeds.json"
STATUS_FILE = ROOT / "feeds_status.json"

_LOCK = threading.Lock()  # protège l'écriture de feeds.json


# Mots-clés utilisés pour filtrer les flux régionaux/nationaux afin
# de ne conserver que les articles touchant le Finistère.
FINISTERE_KEYWORDS = [
    # Département et identité
    "finistère", "finistere", "finisterien", "finisterienne",
    "cornouaille", "léon", "leon", "pays bigouden", "bigouden",
    "monts d'arrée", "monts d'arree", "aulne", "elorn",
    # Grandes villes
    "brest", "brestois", "brestoise",
    "quimper", "quimpérois", "quimperois",
    "morlaix", "morlaisien", "morlaisienne",
    "carhaix", "carhaisien",
    "concarneau", "douarnenez", "landerneau", "landivisiau",
    "quimperlé", "quimperle", "châteaulin", "chateaulin",
    "pleyben", "pont-l'abbé", "pont-l'abbe", "bénodet", "benodet",
    "crozon", "camaret", "plouzané", "plouzane",
    "plougastel", "roscoff", "saint-pol-de-léon", "saint-pol-de-leon",
    "saint-renan", "lesneven", "plabennec", "guipavas",
    "le conquet", "ouessant", "molène", "molene", "sein",
    "fouesnant", "rosporden", "scaër", "scaer",
    "plouescat", "plouguerneau", "plouigneau", "plougonven",
    "pleyber-christ", "saint-thégonnec", "saint-thegonnec",
    "huelgoat", "loctudy", "penmarc'h", "penmarch",
    "audierne", "plogoff", "pont-croix",
    "châteauneuf-du-faou", "chateauneuf-du-faou",
]

CATEGORY_LABELS = {
    "presse-locale": "Presse locale",
    "radio-tv": "Radio & TV",
    "culture": "Culture & patrimoine",
    "independants": "Indépendants",
    "festivals": "Festivals",
    "autres": "Autres",
}

# Villes du Finistère et leurs variantes (toutes en minuscules pour la
# recherche). Utilisé côté UI pour le filtre par ville.
# NB : "Sein" est restreint à "île de sein" pour éviter les faux positifs.
CITIES = {
    "Brest": ["brest", "brestois", "brestoise"],
    "Quimper": ["quimper", "quimpérois", "quimperois"],
    "Morlaix": ["morlaix", "morlaisien", "morlaisienne"],
    "Carhaix": ["carhaix", "carhaisien"],
    "Concarneau": ["concarneau"],
    "Douarnenez": ["douarnenez"],
    "Landerneau": ["landerneau"],
    "Landivisiau": ["landivisiau"],
    "Quimperlé": ["quimperlé", "quimperle"],
    "Châteaulin": ["châteaulin", "chateaulin"],
    "Pleyben": ["pleyben"],
    "Pont-l'Abbé": ["pont-l'abbé", "pont-l'abbe"],
    "Bénodet": ["bénodet", "benodet"],
    "Crozon": ["crozon"],
    "Camaret": ["camaret"],
    "Plouzané": ["plouzané", "plouzane"],
    "Plougastel": ["plougastel"],
    "Roscoff": ["roscoff"],
    "Saint-Pol-de-Léon": ["saint-pol-de-léon", "saint-pol-de-leon"],
    "Saint-Renan": ["saint-renan"],
    "Lesneven": ["lesneven"],
    "Plabennec": ["plabennec"],
    "Guipavas": ["guipavas"],
    "Le Conquet": ["le conquet"],
    "Ouessant": ["ouessant"],
    "Molène": ["molène", "molene"],
    "Île de Sein": ["île de sein", "l'île de sein"],
    "Fouesnant": ["fouesnant"],
    "Rosporden": ["rosporden"],
    "Scaër": ["scaër", "scaer"],
    "Plouescat": ["plouescat"],
    "Plouguerneau": ["plouguerneau"],
    "Plouigneau": ["plouigneau"],
    "Plougonven": ["plougonven"],
    "Pleyber-Christ": ["pleyber-christ"],
    "Saint-Thégonnec": ["saint-thégonnec", "saint-thegonnec"],
    "Huelgoat": ["huelgoat"],
    "Loctudy": ["loctudy"],
    "Penmarc'h": ["penmarc'h", "penmarch"],
    "Audierne": ["audierne"],
    "Plogoff": ["plogoff"],
    "Pont-Croix": ["pont-croix"],
    "Châteauneuf-du-Faou": ["châteauneuf-du-faou", "chateauneuf-du-faou"],
}


def load_feeds() -> list[dict]:
    """Charge la liste des flux depuis feeds.json."""
    if not FEEDS_FILE.exists():
        return []
    with FEEDS_FILE.open(encoding="utf-8") as fh:
        return json.load(fh)


def save_feeds(feeds: list[dict]) -> None:
    """Sauvegarde atomique de feeds.json."""
    with _LOCK:
        tmp = FEEDS_FILE.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(feeds, fh, ensure_ascii=False, indent=2)
        tmp.replace(FEEDS_FILE)


def load_status() -> dict:
    """Charge l'état du dernier fetch (par URL)."""
    if not STATUS_FILE.exists():
        return {}
    try:
        with STATUS_FILE.open(encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_status(status: dict) -> None:
    with _LOCK:
        tmp = STATUS_FILE.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(status, fh, ensure_ascii=False, indent=2)
        tmp.replace(STATUS_FILE)


# Compatibilité : ancien import `from feeds import FEEDS`
FEEDS = load_feeds()
