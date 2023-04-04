"""Abstraction of a parse of a schema definition"""

import copy
import logging
from typing import *
from pydantic import create_model
from sqlmodel import Field, SQLModel
from .const import *
from .exceptions import SchemaError
from .helpers import read_yaml_data


_LOGGER = logging.getLogger(__name__)

__all__ = ["ParsedSchema"]


class ParsedSchema(object):
    # TODO: validate no collision among the 3 namespaces.
    def __init__(self, data: Union[Dict[str, Any], str]) -> None:
        # initial validation and parse
        if not isinstance(data, dict):
            _, data = read_yaml_data(data, "schema")
        data = copy.deepcopy(data)
        self._pipeline_id = _get_or_error(
            data,
            SCHEMA_PIPELINE_ID_KEY,
            f"Missing top-level schema key: '{SCHEMA_PIPELINE_ID_KEY}'",
        )

        # status
        try:
            self._status_data = data.pop("status")
        except KeyError:
            self._status_data = {}
            _LOGGER.debug("No status info found in schema")

        # samples
        try:
            sample_level_data = data.pop("samples")
        except KeyError:
            _LOGGER.debug("No sample-level info found in schema")
            self._sample_level_data = {}
        else:
            if "items" not in sample_level_data:
                raise SchemaError("No 'items' in sample-level schema section")
            sample_level_data = sample_level_data["items"]
            if SCHEMA_PROP_KEY not in sample_level_data:
                raise SchemaError(
                    f"No '{SCHEMA_PROP_KEY}' in sample-level schema items"
                )
            sample_level_data = sample_level_data[SCHEMA_PROP_KEY]
            project_sample_key_overlap = set(data) & set(sample_level_data)
            if project_sample_key_overlap:
                raise SchemaError(
                    f"{len(project_sample_key_overlap)} keys shared between project level and sample level: {', '.join(project_sample_key_overlap)}"
                )
            self._sample_level_data = _recursively_replace_custom_types(
                sample_level_data
            )

        # project-level
        # Now, the main mapping's had some keys removed.
        try:
            prj_data = data.pop(SCHEMA_PROP_KEY)
        except KeyError:
            _LOGGER.debug("No project-level info found in schema")
            self._project_level_data = {}
        else:
            self._project_level_data = _recursively_replace_custom_types(prj_data)

    @property
    def reserved_keywords_used(self):
        reserved_keywords_used = set()
        for data in [self.project_level_data, self.sample_level_data, self.status_data]:
            reserved_keywords_used |= set(data.keys()) & set(RESERVED_COLNAMES)
        return reserved_keywords_used

    @property
    def pipeline_id(self):
        return self._pipeline_id

    @property
    def project_level_data(self):
        return copy.deepcopy(self._project_level_data)

    @property
    def results_data(self):
        return {**self.project_level_data, **self.sample_level_data}

    @property
    def sample_level_data(self):
        return copy.deepcopy(self._sample_level_data)

    @property
    def status_data(self):
        return copy.deepcopy(self._status_data)

    @property
    def project_table_name(self):
        return self._table_name("project")

    @property
    def sample_table_name(self):
        return self._table_name("sample")

    @property
    def status_table_name(self):
        return self._table_name("status")

    def build_project_model(self):
        data = self.project_level_data
        # DEBUG
        print("Project data")
        print(data)
        if data:
            return _create_model(self._table_name("project"), **data)

    def build_sample_model(self):
        data = self.sample_level_data
        if not data:
            return
        id_key = "id"
        sample_fields = {}
        if id_key in data:
            raise SchemaError(
                f"'{id_key}' is reserved for primary key and can't be part of schema."
            )
        sample_fields[id_key] = (
            Optional[int],
            Field(default=None, primary_key=True),
        )
        for field_name, field_data in data.items():
            # TODO: read actual values
            sample_fields[field_name] = (Optional[str], Field(default=None))
        return _create_model(self.sample_table_name, **sample_fields)

    def build_status_model(self):
        data = self.status_data
        if data:
            return _create_model(self._table_name("status"), **data)

    def _table_name(self, suffix: str) -> str:
        return f"{self.pipeline_id}__{suffix}"


def _create_model(table_name: str, **kwargs):
    return create_model(
        table_name, base=SQLModel, __cls_kwargs__={"table": True}, **kwargs
    )


def _get_or_error(data: Dict[str, Any], key: str, msg: Optional[str] = None) -> Any:
    # pre-check avoid potential traceback pollution.
    if key not in data:
        raise SchemaError(msg or f"Missing key: '{key}'")
    return data.pop(key)


def _recursively_replace_custom_types(s: dict) -> Dict:
    """
    Replace the custom types in pipestat schema with canonical types

    :param dict s: schema to replace types in
    :return dict: schema with types replaced
    """
    for k, v in s.items():
        missing_req_keys = [
            req for req in [SCHEMA_TYPE_KEY, SCHEMA_DESC_KEY] if req not in v
        ]
        if missing_req_keys:
            raise SchemaError(
                f"Result '{k}' is missing required key(s): {', '.join(missing_req_keys)}"
            )
        curr_type_name = v[SCHEMA_TYPE_KEY]
        if curr_type_name == "object" and SCHEMA_PROP_KEY in s[k]:
            _recursively_replace_custom_types(s[k][SCHEMA_PROP_KEY])
        try:
            curr_type_spec = CANONICAL_TYPES[curr_type_name]
        except KeyError:
            continue
        s.setdefault(k, {})
        s[k].setdefault(SCHEMA_PROP_KEY, {})
        s[k][SCHEMA_PROP_KEY].update(curr_type_spec[SCHEMA_PROP_KEY])
        s[k].setdefault("required", []).extend(curr_type_spec["required"])
        s[k][SCHEMA_TYPE_KEY] = curr_type_spec[SCHEMA_TYPE_KEY]
    return s
