"""
Platform-specific JSONPath configurations for user data extraction.

This module provides platform-specific JSONPath patterns for extracting
and protecting user identifiable information from social media post content.
"""

from typing import Literal

platform_name = Literal["twitter", "instagram", "weibo", "tiktok", "youtube"]


def platform_user_data_jsonpath(platform: platform_name) -> tuple[str, list[str]]:
    """
    Get JSONPath patterns for user data extraction based on platform.

    Returns the primary user ID path and additional metadata paths that
    should be protected during anonymization.

    Parameters
    ----------
    platform : platform_name
        The social media platform name. Supported: twitter, instagram,
        weibo, tiktok, youtube.

    Returns
    -------
    tuple[str, list[str]]
        A tuple containing:
        - user_id_path: JSONPath to the primary user identifier
        - metadata_paths: List of JSONPaths to additional sensitive fields

    Examples
    --------
    >>> user_id_path, metadata_paths = platform_user_data_jsonpath("twitter")
    >>> print(user_id_path)
    'user.id_str'
    >>> print(metadata_paths)
    ['url', 'user.id', 'user.url', 'user.username', 'user.displayname']
    """
    match platform:
        case "youtube":
            return "snippet.channelId", ["snippet.channelTitle"]
        case "twitter":
            return "user.id_str", ["url", "user.id", "user.url", "user.username", "user.displayname"]
        case "tiktok":
            return "username", []
        case "instagram":
            return "post_owner.id", ["post_owner.type", "post_owner.name", "post_owner.username"]
        case _:
            print("warning: unknown platform", platform)
            return "", []



