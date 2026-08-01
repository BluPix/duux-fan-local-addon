"""Multi-language entity translations loaded dynamically from JSON files."""

import json
import logging
from pathlib import Path

_LOGGER = logging.getLogger(__name__)

TRANSLATIONS_DIR = Path(__file__).parent / "translations"


def load_translations() -> dict[str, dict[str, str]]:
    """Load entity translation JSON files (cs.json, de.json, nl.json, etc.)."""
    translations: dict[str, dict[str, str]] = {}
    if not TRANSLATIONS_DIR.exists():
        _LOGGER.warning("Translations directory %s does not exist", TRANSLATIONS_DIR)
        return translations

    for json_file in TRANSLATIONS_DIR.glob("*.json"):
        lang_code = json_file.stem.lower()
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                translations[lang_code] = json.load(f)
                _LOGGER.debug("Loaded entity translations for language '%s'", lang_code)
        except Exception as err:
            _LOGGER.warning("Could not load translation file %s: %s", json_file, err)

    return translations


TRANSLATIONS = load_translations()
