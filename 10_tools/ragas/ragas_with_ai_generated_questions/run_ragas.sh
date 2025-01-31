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
    "01_ragas_generate_questions_v0.1.py"
    "02_ragas_evaluation_v0.2.py"

)

# Ausführen der Skripte
for script in "${scripts[@]}"; do
    echo -e "Starte ${GREEN}${script}${NOCOLOR}..."
    python3 "$script"
done
echo "Alle Skripte wurden erfolgreich ausgeführt!"
