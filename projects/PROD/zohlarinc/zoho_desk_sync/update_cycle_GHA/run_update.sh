#!/bin/bash

# Stoppt das Skript bei einem Fehler
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NOCOLOR='\033[0m'

# Prüfen der AWS CLI-Konnektivität (optional, falls AWS-Dienste genutzt werden)
if ! aws s3 ls > /dev/null; then
    echo -e "${RED}Fehler: AWS CLI ist nicht richtig konfiguriert.${NOCOLOR}"
    exit 1
else
    echo "AWS CLI ist erfolgreich konfiguriert."
fi


scripts=(
    "01_get_zohodata_v2.0.py"
    "02_convert_zohodata_to_vectordata_format_v2.0.py"
    "03_sync_zohodata_with_vectordata_v2.0.py"
    "04_identify_synced_vectordata_to_enrich_v2.0.py"
    "05_update_vectordata_v2.0.py"
    "06a_upload_to_postgres_with_titan_v2.0.py"
)

# Ausführen der Skripte
for script in "${scripts[@]}"; do
    echo -e "Starte ${GREEN}${script}${NOCOLOR}..."
    python3 "$script"
done
echo "Alle Skripte wurden erfolgreich ausgeführt!"
