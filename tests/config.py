import logging
import uuid
from typing import Text

from rich.console import Console

from simple_pymq.config import get_os_env


class Settings:
    # General Config
    logger_name: Text = "pytest"
    test_uuid: uuid.UUID = get_os_env(
        "test_uuid", default=uuid.uuid4(), return_type="uuid"
    )

    # Redis Config
    test_redis_host: Text = get_os_env(
        "test_redis_host", default=None, return_type="str"
    )
    test_redis_port: int = get_os_env(
        "test_redis_port", default=None, return_type="int"
    )
    test_redis_db: int = get_os_env("test_redis_db", default=None, return_type="int")
    test_redis_username: Text = get_os_env(
        "test_redis_username", default=None, return_type="str"
    )
    test_redis_password: Text = get_os_env(
        "test_redis_password", default=None, return_type="str"
    )

    # AMQP Config
    test_amqp_url: Text = get_os_env("test_amqp_url", default=None, return_type="str")


settings = Settings()
console = Console()
logger = logging.getLogger(settings.logger_name)
