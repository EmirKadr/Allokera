# Gör projektroten till paket och exponera version + vanliga imports om du vill.
__version__ = "5.0"

# Valfria bekvämlighetsimporter (håll minimalt för att undvika tunga importer)
from .config.constants import APP_TITLE  # noqa:F401

__all__ = ["__version__", "APP_TITLE"]
