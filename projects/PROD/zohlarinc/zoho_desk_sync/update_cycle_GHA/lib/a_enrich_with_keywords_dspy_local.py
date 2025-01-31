import json
import os
import logging
from tqdm import tqdm
import dspy
from dotenv import load_dotenv
import unicodedata

# Lokales Verzeichnis für die Ein- und Ausgabe von Dateien
BASE_DIR = os.getenv('BASE_DIR')

# Logging einrichten
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Umgebungsvariablen laden
load_dotenv()

def normalize_unicode_characters(data):
    """Unicode-Zeichen in den Daten rekursiv normalisieren."""
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

def load_json_from_file(file_path):
    """Lädt JSON-Daten aus einer Datei."""
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def save_json_to_file(data, file_path):
    """Speichert JSON-Daten in einer Datei."""
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


dspy.configure(lm=dspy.OpenAI(model='gpt-4', api_key=os.getenv('OPENAI_API_KEY')))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ExtractKeywords(dspy.Signature):
    """Extrahiere bis zu vier relevante Schlüsselwörter aus dem Inhalt."""
    content = dspy.InputField()
    keywords = dspy.OutputField()


extract_keywords_model = dspy.Predict(ExtractKeywords)

# Transformiert die alte Struktur in die neue Struktur


def transform_structure(old_content):

    keywords_response = extract_keywords_model(content=old_content['answer'])
    keywords = keywords_response.keywords.split(', ')  # Annahme: Keywords sind als kommagetrennter String zurückgegeben

def transform_structure(old_content):
    """Verwenden von dspy, um Keywords zu extrahieren und die Datenstruktur zu transformieren."""
    keywords_response = extract_keywords_model(content=old_content['answer'])
    keywords = keywords_response.keywords.split(', ')

    new_content = {
"namespace": "",
    "id": old_content.get("id"),
    "title": old_content.get('title', ''),
    "answer": old_content.get('answer', ''),
    "link": old_content.get("metadata", {}).get("zd_metadata", {}).get("webUrl"),
    "parent": "",
    "keywords": keywords,
    "meta_description": old_content.get("metadata", {}).get("zd_metadata", {}).get("summary"),
    "combined_text": old_content.get("combined_text", ""),
    "metadata": {
        "category": old_content.get("metadata", {}).get("category", ""),
        "sub_category": old_content.get("metadata", {}).get("sub_category", ""),
        "tags": old_content.get("metadata", {}).get("tags", []),
        "last_updated": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifiedTime"),
        "author": old_content.get("metadata", {}).get("zd_metadata", {}).get("author", {}).get("name", ""),
        "views": old_content.get("metadata", {}).get("zd_metadata", {}).get("viewCount"),
        "like": old_content.get("metadata", {}).get("zd_metadata", {}).get("likeCount"),
        "difficulty_level": old_content.get("metadata", {}).get("difficulty_level", ""),
        "version": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestVersion"),
        "related_links": old_content.get("metadata", {}).get("related_links", []),
        "zd_metadata": {
            "modifiedTime": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifiedTime"),
            "departmentId": old_content.get("metadata", {}).get("zd_metadata", {}).get("departmentId"),
            "creatorId": old_content.get("metadata", {}).get("zd_metadata", {}).get("creatorId"),
            "dislikeCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("dislikeCount"),
            "modifierId": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifierId"),
            "likeCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("likeCount"),
            "locale": old_content.get("metadata", {}).get("zd_metadata", {}).get("locale"),
            "ownerId": old_content.get("metadata", {}).get("zd_metadata", {}).get("ownerId"),
            "title": old_content.get("metadata", {}).get("zd_metadata", {}).get("title"),
            "translationState": old_content.get("metadata", {}).get("zd_metadata", {}).get("translationState"),
            "isTrashed": old_content.get("metadata", {}).get("zd_metadata", {}).get("isTrashed"),
            "createdTime": old_content.get("metadata", {}).get("zd_metadata", {}).get("createdTime"),
            "modifiedBy": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifiedBy"),
            "id": old_content.get("metadata", {}).get("zd_metadata", {}).get("id"),
            "viewCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("viewCount"),
            "translationSource": old_content.get("metadata", {}).get("zd_metadata", {}).get("translationSource"),
            "owner": old_content.get("metadata", {}).get("zd_metadata", {}).get("owner"),
            "summary": old_content.get("metadata", {}).get("zd_metadata", {}).get("summary"),
            "latestVersionStatus": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestVersionStatus"),
            "author": old_content.get("metadata", {}).get("zd_metadata", {}).get("author"),
            "permission": old_content.get("metadata", {}).get("zd_metadata", {}).get("permission"),
            "authorId": old_content.get("metadata", {}).get("zd_metadata", {}).get("authorId"),
            "usageCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("usageCount"),
            "commentCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("commentCount"),
            "rootCategoryId": old_content.get("metadata", {}).get("zd_metadata", {}).get("rootCategoryId"),
            "sourceLocale": old_content.get("metadata", {}).get("zd_metadata", {}).get("sourceLocale"),
            "translationId": old_content.get("metadata", {}).get("zd_metadata", {}).get("translationId"),
            "createdBy": old_content.get("metadata", {}).get("zd_metadata", {}).get("createdBy"),
            "latestVersion": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestVersion"),
            "webUrl": old_content.get("metadata", {}).get("zd_metadata", {}).get("webUrl"),
            "feedbackCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("feedbackCount"),
            "portalUrl": old_content.get("metadata", {}).get("zd_metadata", {}).get("portalUrl"),
            "attachmentCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("attachmentCount"),
            "latestPublishedVersion": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestPublishedVersion"),
            "position": old_content.get("metadata", {}).get("zd_metadata", {}).get("position"),
            "availableLocaleTranslations": old_content.get("metadata", {}).get("zd_metadata", {}).get("availableLocaleTranslations", []),
            "category": old_content.get("metadata", {}).get("zd_metadata", {}).get("category"),
            "permalink": old_content.get("metadata", {}).get("zd_metadata", {}).get("permalink"),
            "categoryId": old_content.get("metadata", {}).get("zd_metadata", {}).get("categoryId"),
            "status": old_content.get("metadata", {}).get("zd_metadata", {}).get("status"),
            "tags": old_content.get("metadata", {}).get("zd_metadata", {}).get("tags", [])
                }
            }
        }

    return new_content

def enrich_with_keywords(input_path, output_path):
    """Verarbeitet Daten lokal und speichert das Ergebnis."""
    try:
        old_contents = load_json_from_file(input_path)
        new_contents = [transform_structure(content) for content in tqdm(old_contents, desc="Verarbeite Datensätze")]
        normalized_contents = [normalize_unicode_characters(content) for content in new_contents]
        save_json_to_file(normalized_contents, output_path)
        logging.info(f"Verarbeitete Daten erfolgreich gespeichert in: {output_path}")
    except Exception as e:
        logging.error(f"Fehler beim Verarbeiten der Daten: {e}")

def main():
    input_path = os.path.join(BASE_DIR, '04_synced_vectordata_without_both.json')
    output_path = os.path.join(BASE_DIR, '04_synced_vectordata_without_both_processed.json')

    enrich_with_keywords(input_path, output_path)

if __name__ == "__main__":
    main()