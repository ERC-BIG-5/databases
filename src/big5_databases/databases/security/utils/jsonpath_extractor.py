"""
JSONPath field extractor for the security module.

Copied from datapipeline.misc.util to avoid external dependencies.
This provides robust JSONPath extraction with validation and error handling.
"""

from typing import Any, Union
import jsonpath_ng # type: ignore
from jsonpath_ng.parser import JsonPathParser  # type: ignore
from jsonpath_ng.exceptions import JsonPathParserError # type: ignore
from pydantic import BaseModel, RootModel, field_validator


class JsonPathFieldExtractor(RootModel[dict[str, str]]):
    """
    Model-based JSONPath field extractor that validates paths at configuration time
    and provides robust data extraction with proper error handling.

    Example usage:
        extractor = JsonPathFieldExtractor({
            "user_id": "$.user.id_str",
            "username": "$.user.screen_name",
            "content": "$.text"
        })
        data = extractor.extract_from_data(post_content)
    """
    root: dict[str, str]

    @field_validator("root")
    @classmethod
    def validate_jsonpath_expressions(cls, field_paths: dict[str, str]) -> dict[str, str]:
        """Validate all JSONPath expressions at configuration time"""
        for field_name, json_path in field_paths.items():
            try:
                jsonpath_ng.parse(json_path)
            except JsonPathParserError as err:
                raise ValueError(f"Invalid JSONPath for field '{field_name}': {json_path}. Error: {err}")
        return field_paths

    def __init__(self, field_paths: dict[str, str]):
        """Initialize with field name to JSONPath mapping"""
        super().__init__(field_paths)
        # Pre-parse all JSONPath expressions for performance
        self._parsers: dict[str, JsonPathParser] = {
            field_name: jsonpath_ng.parse(json_path)
            for field_name, json_path in self.root.items()
        }

    def extract_from_data(self, data: dict) -> dict[str, Any]:
        """Extract field data from a dictionary using configured JSONPath expressions"""
        return self._extract_from_data(data)

    def _extract_from_data(self, data: dict) -> dict[str, Any]:
        """Internal method for data extraction with null filtering"""
        result = {}
        for field_name, parser in self._parsers.items():
            matches = [match.value for match in parser.find(data)]
            # Filter out None values for data quality
            matches = [m for m in matches if m is not None]
            result[field_name] = matches[0] if matches else None
        return result

    def get_parser(self, field_name: str) -> JsonPathParser:
        """Get the pre-parsed JSONPath parser for a specific field"""
        if field_name not in self._parsers:
            raise KeyError(f"Field '{field_name}' not configured in extractor")
        return self._parsers[field_name]

    def replace_in_data(self, data: dict, field_name: str, replacement_value: Any) -> dict:
        """
        Replace values at JSONPath locations using robust jsonpath-ng update method.

        Args:
            data: Dictionary to modify
            field_name: Field name (must be configured in extractor)
            replacement_value: Value to replace with (e.g., "<PROTECTED>")

        Returns:
            Modified dictionary (modifies original dict in-place)
        """
        if field_name not in self._parsers:
            raise KeyError(f"Field '{field_name}' not configured in extractor")

        parser = self._parsers[field_name]

        # Use jsonpath-ng's native update method - this handles all path types
        # including simple paths, nested paths, arrays, and wildcards
        try:
            parser.update(data, replacement_value)
        except Exception:
            # Silently skip if path cannot be updated (e.g., field doesn't exist)
            # This is expected behavior for optional fields
            pass

        return data


    @property
    def field_names(self) -> list[str]:
        """Get list of configured field names"""
        return list(self.root.keys())

    @property
    def json_paths(self) -> dict[str, str]:
        """Get the configured field name to JSONPath mapping"""
        return self.root.copy()


class JsonPathContentProtector:
    """
    Helper class to protect sensitive content in JSON data by replacing
    values at specified JSONPath locations with protected strings.
    """

    def __init__(self, protection_paths: dict[str, str], replacement: str = "<PROTECTED>"):
        """
        Initialize content protector.

        Args:
            protection_paths: Dict mapping field names to JSONPath expressions
            replacement: String to replace sensitive values with
        """
        self.extractor = JsonPathFieldExtractor(protection_paths)
        self.replacement = replacement

    def protect_data(self, data: dict) -> dict:
        """
        Protect sensitive data by replacing values with protection string.

        Args:
            data: Dictionary to protect (modified in-place)

        Returns:
            Protected dictionary
        """
        data_copy = data.copy()
        for field_name in self.extractor.field_names:
            self.extractor.replace_in_data(data_copy, field_name, self.replacement)
        return data_copy