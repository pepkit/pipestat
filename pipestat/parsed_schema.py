"""Abstraction of a parse of a schema definition"""

import copy
import json
import logging
from pathlib import Path
from typing import *
#from pydantic import BaseModel, create_model
from pydantic import create_model

# from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Field, SQLModel
from .const import *
from .exceptions import SchemaError
from .helpers import read_yaml_data


_LOGGER = logging.getLogger(__name__)

__all__ = ["ParsedSchema"]


# The columns associated with the file and image types
PATH_COL_SPEC = (Path, ...)
TITLE_COL_SPEC = (Optional[str], Field(default=None))
THUMBNAIL_COL_SPEC = (Optional[Path], Field(default=None))


def _custom_types_column_specifications():
    """Collection of the column specifications for the custom types"""
    return {"path": PATH_COL_SPEC, "title": TITLE_COL_SPEC, "thumbnail": THUMBNAIL_COL_SPEC}


def _add_custom_types_columns(col_specs: Dict[str, Any]) -> Dict[str, Any]:
    custom_specs = {"path": PATH_COL_SPEC, "title": TITLE_COL_SPEC, "thumbnail": THUMBNAIL_COL_SPEC}
    collisions = set(custom_specs) & set(col_specs)
    if collisions:
        raise SchemaError(f"{len(collisions)} reserved column name(s) used in schema: {', '.join(collisions)}")
    return {**col_specs, **custom_specs}


class BaseModel(SQLModel):
    class Config:
        arbitrary_types_allowed = True


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

    def _make_field_definitions(self, data: Dict[str, Any]):
        # TODO: read actual values
        # TODO: default to string if no type key?
        # TODO: parse "required" ?
        defs = {}
        for name, subdata in data.items():
            typename = subdata[SCHEMA_TYPE_KEY]
            if typename in CANONICAL_TYPES:
                # these are handled separately, table-wise (file-likes)
                continue
            defs[name] = (
                # Optional[subdata[SCHEMA_TYPE_KEY]],
                # subdata[SCHEMA_TYPE_KEY],
                # Optional[str],
                # CLASSES_BY_TYPE[subdata[SCHEMA_TYPE_KEY]],
                self._get_data_type(typename),
                Field(default=subdata.get("default")),
            )
        return defs

    @staticmethod
    def _get_data_type(type_name):
        t = CLASSES_BY_TYPE[type_name]
        # return ARRAY if t == list else t
        return t

    @property
    def file_like_table_name(self):
        return self._table_name("files")

    def build_file_model(self):
        name = self.file_like_table_name
        field_defs = _add_custom_types_columns({})
        # TODO: check that this key isn't already there.
        field_defs[RECORD_ID_KEY] = (str, ...)
        self._add_id_field(field_defs)
        return _create_model(name, **field_defs)

    def build_project_models(self):
        """Create the models associated with project-level data."""
        data = self.project_level_data
        # DEBUG
        print("DATA")
        print(json.dumps(data, indent=2))
        field_defs = self._make_field_definitions(data)
        self._add_id_field(field_defs)
        # DEBUG
        print("FIELD DEFS")
        print(field_defs)
        if not field_defs:
            # DEBUG
            print("NO FIELD DEFINITIONS!")
            return None
        scalar_model = _create_model(self.project_table_name, **field_defs)
        files_model = self.build_file_model()
        return {"scalars": scalar_model, "files": files_model}

    def build_sample_model(self):
        # TODO: include the ability to process the custom types.
        # TODO: at minimum, we need capability for image and file, and maybe link.
        raise NotImplementedError("sample-level isn't yet integrated")
        data = self.sample_level_data
        if not data:
            return
        sample_fields = self._make_field_definitions(data)
        self._add_id_field(sample_fields)
        return _create_model(self.sample_table_name, **sample_fields)

    @staticmethod
    def _add_id_field(field_defs: Dict[str, Any]) -> None:
        id_key = "id"
        if id_key in field_defs:
            raise SchemaError(
                f"'{id_key}' is reserved for primary key and can't be part of schema."
            )
        field_defs[id_key] = (
            Optional[int],
            Field(default=None, primary_key=True),
        )

    def build_status_model(self):
        field_defs = self._make_field_definitions(self.status_data)
        if field_defs:
            return _create_model(self.status_table_name, **field_defs)

    def _table_name(self, suffix: str) -> str:
        return f"{self.pipeline_id}__{suffix}"


def _create_model(table_name: str, **kwargs):
    #return create_model(table_name, __base__=BaseModel, **kwargs)
    return create_model(
        table_name, __base__=BaseModel, __cls_kwargs__={"table": True}, **kwargs
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
        # DEBUG
        print(f"curr_type_name: {curr_type_name}")
        if curr_type_name == "object" and SCHEMA_PROP_KEY in s[k]:
            # TODO: are we still supporting this if switching to SQLModel?
            # DEBUG
            print("recursing")
            _recursively_replace_custom_types(s[k][SCHEMA_PROP_KEY])
        try:
            curr_type_spec = CANONICAL_TYPES[curr_type_name]
        except KeyError:
            # DEBUG
            print(f"Not a canonical type: {curr_type_name}")
            continue
        # DEBUG
        print("Current type spec")
        print(curr_type_spec)
        spec = s.setdefault(k, {})
        spec.setdefault(SCHEMA_PROP_KEY, {}).update(curr_type_spec[SCHEMA_PROP_KEY])
        spec.setdefault("required", []).extend(curr_type_spec["required"])
        spec[SCHEMA_TYPE_KEY] = curr_type_spec[SCHEMA_TYPE_KEY]
    return s
