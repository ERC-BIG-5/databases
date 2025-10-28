from pathlib import Path
from typing import Literal

from pydantic import BaseModel, field_validator, ValidationError

platform_name = Literal["twitter", "instagram", "weibo", "tiktok", "youtube"]


def platform_user_data_jsonpath(platform: platform_name) -> tuple[str, list[str]]:
    match platform:
        case "youtube":
            return "snippet.channelId", ["snippet.channelTitle"]
        case "twitter":
            return "user.id_str", ["user.id", "user.url", "user.username", "user.displayname"]
        case "tiktok":
            return "username", []
        case "instagram":
            return "post_owner.id", ["post_owner.type", "post_owner.name", "post_owner.username"]
        case _:
            print("warning: unknown platform", platform)
            return "", []


class AnonFixDBConfig(BaseModel):
    path: Path
    platform: platform_name

    @field_validator("path", mode="after")
    @classmethod
    def db_exists(cls, v: Path) -> Path:
        if not v.exists():
            raise ValidationError(f"Database {v} does not exist")
        return v


class AnonFixConfig:
    databases: list[AnonFixDBConfig]


if __name__ == "__main__":
    pass