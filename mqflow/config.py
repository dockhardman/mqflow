import logging
from typing import Text

from rich.console import Console


class Settings:
    # Common
    logger_name: Text = "mqflow"


settings = Settings()
logger = logging.getLogger(settings.logger_name)
console = Console()
