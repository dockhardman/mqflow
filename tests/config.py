import uuid

from simple_pymq.config import get_os_env


class Settings:
    test_uuid: uuid.UUID = get_os_env(
        "test_uuid", default=uuid.uuid4(), return_type="uuid"
    )


settings = Settings()
