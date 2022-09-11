from __future__ import annotations

import re
from typing import Any, Callable, Generic, Type, TypeVar

from . import models, utils


D = TypeVar('D')
M = TypeVar('M')


class Adapter(Generic[D]):
    def __init__(self, attributes_converter: Callable[[str], str] | None = utils.to_snake_case):
        self.attributes_converter = attributes_converter

    def get_raw_data(self, data: D, model: Type[M]) -> dict[str, Any]:
        raise NotImplementedError

    def parse(self, data: D, model: Type[M]) -> M:
        raw_data = self.get_raw_data(data, model)
        raw_data = self.convert_attribute_names(raw_data)
        cleaned_data = self.full_clean(raw_data, model)
        return model(**cleaned_data)

    def convert_attribute_names(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        if not self.attributes_converter:
            return raw_data

        return {
            self.attributes_converter(key): value
            for key, value in raw_data.items()
        }

    def full_clean(self, raw_data: dict[str, Any], model: Type[M]) -> dict[str, Any]:
        return self.clean(self._clean_fields(raw_data, model), model)

    def clean(self, raw_data: dict[str, Any], model: Type[M]) -> dict[str, Any]:
        return raw_data

    def _clean_fields(self, raw_data: dict[str, Any], model: Type[M]) -> dict[str, Any]:
        cleaned_data = {}
        for key, value in raw_data.items():
            cleaner = f'clean_{key}'
            field = model._meta.fields.get(key, None)
            if hasattr(self, cleaner):
                cleaned_data[key] = getattr(self, cleaner)(raw_data, value, model, field)
            else:
                if field:
                    # We use the default field conversion
                    cleaned_data[key] = field.to_python(self, value)
                else:
                    cleaned_data[key] = value
        return cleaned_data


class DictAdapter(Adapter[dict]):
    """
    Dummy adapter simply mapping dict keys to model attributes
    """

    def __init__(self, *args, recursive: bool = True, key: str | None = None, **kwargs):
        self.recursive = recursive
        # if any, we'll map only attributes under the given key
        self.key = key
        super(DictAdapter, self).__init__(*args, **kwargs)

    def get_raw_data(self, data: dict[str, Any], model) -> dict[str, Any]:
        if self.key:
            to_convert = data[self.key]
        else:
            to_convert = data

        if self.recursive:
            # we convert subdictionaries to proper model instances
            for key, value in to_convert.items():
                if isinstance(value, dict):
                    to_convert[key] = self.parse(value, models.Model)

        return to_convert


class RegexAdapter(Adapter[str]):
    regex: str | re.Pattern | None = None
    compiled_regex: re.Pattern

    def __init__(self, regex: str | re.Pattern | None = None, **kwargs):
        self.regex = regex or self.regex
        if self.regex is None:
            raise ValueError('Please provide a regex pattern')
        self.compiled_regex = re.compile(self.regex)
        super(RegexAdapter, self).__init__(**kwargs)

    def get_raw_data(self, data: str, model) -> dict[str, Any]:
        match = self.compiled_regex.match(data)
        return match.groupdict()


class ETreeAdapter(Adapter):

    def get_raw_data(self, data, model) -> dict[str, Any]:
        return {
            self.tag_to_field_name(e.tag): e.text
            for e in data
        }

    def tag_to_field_name(self, tag: str) -> str:
        """
        Since the tag may be fully namespaced, we want to strip the namespace
        information
        """
        return tag.split('}')[-1]
