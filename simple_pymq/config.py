import os
import logging
from pathlib import Path
from typing import Any, Optional, Text, Union


def get_os_env(
    key: Text, *args, default: Any, return_type: Text = "str"
) -> Optional[Union[Text, bool, int, float, Path]]:
    return_type = return_type.casefold()
    value = os.environ.get(key)

    if value is None:
        valid_value = default
    elif return_type == "str" or return_type == "text":
        valid_value = value
    elif return_type == "int":
        valid_value = int(value)
    elif return_type == "float":
        valid_value = float(value)
    elif return_type == "path":
        valid_value = Path(value)
    elif return_type == "bool":
        valid_value = bool(value)
    else:
        raise ValueError(f"Unknown return type: '{return_type}'")

    return valid_value


class Settings:
    # Common
    logger_name: Text = get_os_env("logger_name", default="simple-pymq")


settings = Settings()
logger = logging.getLogger(settings.logger_name)
