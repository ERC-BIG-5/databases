from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import Field

# not sure if this is needed, cuz or root()

class Settings(BaseSettings):
    main_db_path: Optional[Path] = Field(None, alias="MAIN_DB_PATH")

SETTINGS = Settings()