import gettext
import os
import locale as sys_locale
from pathlib import Path
from typing import Optional

# Global catalog
_current_translation: Optional[gettext.NullTranslations] = None


def setup_i18n(language: Optional[str] = None):
    """
    Initializes the translation system.

    Args:
        language: 'es' or 'en'. If None, attempts to detect system locale.
    """
    global _current_translation

    # 1. Determine language
    if not language:
        # Fallback to system env or default to 'es'
        # Windows: locale.getdefaultlocale() -> ('es_ES', 'cp1252')
        try:
            lang, _ = sys_locale.getdefaultlocale()
            language = lang[:2] if lang else "es"
        except:
            language = "es"

    # 2. Locate locales directory
    # Assumes: src/yupay/core/i18n.py -> src/yupay/locales
    base_dir = Path(__file__).parent.parent / "locales"

    # 3. Load catalog
    # Fallback=True ensures that if 'es' fails, it falls back to built-in strings (English keys)
    # 3. Load catalog
    # Fallback=True ensures that if 'es' fails, it falls back to built-in strings (English keys)
    try:
        _current_translation = gettext.translation(
            domain="messages",
            localedir=str(base_dir),
            languages=[language],
            fallback=True
        )
    except FileNotFoundError:
        # If .mo files are missing, fallback to Null (return keys as-is)
        _current_translation = gettext.NullTranslations()

    # 4. Install _() globally (Optional, but convenient for some setups)
    # We prefer explicit imports, so we won't do install() automatically
    # _current_translation.install()


def get_translation_func():
    """
    Returns the _() function for the current active transaction.
    If setup_i18n hasn't been called, it returns a pass-through.
    """
    if _current_translation:
        return _current_translation.gettext
    return lambda x: x  # Pass-through


# Singleton accessor
def _(message: str) -> str:
    """
    Proxy for the active translation.
    Usage: from yupay.core.i18n import _
    console.print(_("Hello World"))
    """
    if _current_translation is None:
        # Lazy init if not setup
        setup_i18n()
    return _current_translation.gettext(message)
