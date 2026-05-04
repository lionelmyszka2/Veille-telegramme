#!/bin/bash
# Mise à jour des articles puis ouverture du tableau de bord (mode statique).
# Pour gérer les flux (onglet Admin), utilise plutôt ./serve.sh
set -e
cd "$(dirname "$0")"
python3 veille.py
open veille.html
