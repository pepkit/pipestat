import os
import sys
import logging
import logmuse
import oyaml as yaml

from collections import Mapping
from copy import copy

from .exceptions import *
from .const import *
from .helpers import *


_LOGGER = logging.getLogger(__name__)

"""
This does not work for some reason for mongoDB:


In [12]: psm = pipestat.PipeStatManager(pipestat.connect_mongo(), "test")                                                                                                                                 

In [13]: psm.database                                                                                                                                                                                     
Out[13]: {'TEST': {'value': {}}}

In [14]: psm.report(id="AAA", value="new", type="string")                                                                                                                                                 
Cached new 'test' record: AAA=new(string)
Wrote 1 cached records: {'test': {'AAA': {'value': 'new', 'type': 'string'}}}
Out[14]: True

In [15]: psm.database                                                                                                                                                                                     
Out[15]: {'TEST': {'value': {}}, 'test': {}} <-- no 'test' namespace content
"""


class PipeStatManager(object):
    def __init__(self, database, name):
        self._name = str(name)
        self._cache = dict()
        self._db_file = None

        if isinstance(database, str):
            if os.path.exists(database):
                with open(database, "r") as db_stream:
                    self._database = yaml.safe_load(db_stream) or {}
                    self._db_file = database
            else:
                raise FileNotFoundError(
                    "Provided database file path does not exist: {}".
                        format(database))
        elif isinstance(database, Mapping):
            self._database = database
        else:
            raise NotImplementedError(
                "'{}' is not a supported database type".
                    format(database.__class__.__name__))

    @property
    def name(self):
        """
        namespace name

        :return str: name of the namespace that results are reported for
        """
        return self._name

    @property
    def database(self):
        """
        Database contents

        :return dict: contents of the database
        """
        return dict(self._database)

    @property
    def cache(self):
        """
        Cache contents

        :return dict: contents of the cache
        """
        return self._cache

    def report(self, id, type, value, cache=False, overwrite=False):
        """
        Report a result

        :param str id: unique name of the item to report
        :param str type: type of the item to report
        :param str value: value of the item to report
        :param bool cache: whether just cache the reported item, not write it yet
        :param bool overwrite: whether to overwrite the item in case of id clashes
        :return bool: whether the result was reported or not
        """
        if type not in TYPES:
            raise InvalidTypeError(type)
        self._database.setdefault(self.name, {})
        self._cache.setdefault(self.name, {})
        if id in self.database[self.name]:
            _LOGGER.warning("'{}' already in database for '{}' namespace"
                            .format(id, self.name))
            if not overwrite:
                return False
        if id in self.cache[self.name]:
            _LOGGER.warning("'{}' already in cache for '{}' namespace"
                            .format(id, self.name))
            if not overwrite:
                return False
        self._cache[self.name].setdefault(id, {})
        self._cache[self.name][id]["value"] = value
        self._cache[self.name][id]["type"] = type
        _LOGGER.info("Cached new '{}' record: {}={}({})".
                     format(self.name, id, value, type))
        if not cache:
            self.write()
        return True

    def remove(self, id):
        """
        Remove a result

        :param str id: the name of the result to remove
        :return bool: whether the result was removed or not
        """
        def _rm_res(instance, id, name):
            if name not in instance:
                return False
            if id not in instance[name]:
                return False
            del instance[name][id]
            return True

        removed = False
        if self.name not in self.database and self.name not in self.cache:
            _LOGGER.warning("namespace '{}' not found".format(self.name))
            return removed
        removed = any([_rm_res(self._database, id, self.name),
                       _rm_res(self._cache, id, self.name)])
        if not removed:
            _LOGGER.warning("'{}' has not been reported for '{}' namespace".
                            format(id, self.name))
        return removed

    def write(self):
        """
        Write reported results to the database
        """
        self._database.setdefault(self.name, {})
        self._database[self.name].update(self._cache[self.name])
        if self._db_file:
            with open(self._db_file, "w") as db_stream:
                yaml.dump(self._database, db_stream, default_flow_style=False)
        _LOGGER.info("Wrote {} cached records: {}".
                     format(len(self.cache), self.cache))
        self._cache = {}

    def __str__(self, max_len=20):
        res = "{} ({})".format(self.__class__.__name__, self.name)
        pip_db_len = 0 if self.name not in self.database \
            else len(self.database[self.name])
        pip_cache_len = 0 if self.name not in self.cache \
            else len(self.cache[self.name])
        res += "\nDatabase length: {}".format(pip_db_len)
        if pip_db_len < max_len:
            res += "\nDatabase: {}".format(dict(self.database))
        res += "\nCache length: {}".format(pip_cache_len)
        if pip_cache_len < max_len:
            res += "\nCache: {}".format(dict(self.cache))
        return res

    def __repr__(self):
        return "{name: " + self.name + ", " + \
               "database: " + dict(self.database).__repr__() + ", " + \
               "cache: " + dict(self.cache).__repr__() + "}"


def main():
    """ Primary workflow """
    parser = logmuse.add_logging_options(build_argparser())
    args = parser.parse_args()
    if args.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    global _LOGGER
    _LOGGER = logmuse.logger_via_cli(args, make_root=True)
    _LOGGER.debug("Args namespace:\n{}".format(args))
    psm = PipeStatManager(database=args.database, name=args.name)
    if args.command == "report":
        psm.report(id=args.id, type=args.type, value=args.value,
                   overwrite=args.overwrite)