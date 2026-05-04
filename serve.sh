#!/bin/bash
# Lance le serveur local de la veille (avec onglet Admin) et ouvre le navigateur.
set -e
cd "$(dirname "$0")"
exec python3 serve.py "$@"
