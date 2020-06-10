import os
import logging
import logmuse
import oyaml as yaml

from collections import Mapping
from copy import copy

from .exceptions import *
from .const import *
from .helpers import *


_LOGGER = logging.getLogger(__name__)


class PipeStatManager(object):
    def __init__(self, database, name):
        self.name = name
        self._cache = dict()
        self._db_file = None

        if isinstance(database, str):
            if os.path.exists(database):
                with open(database, "r") as db_stream:
                    self._database = yaml.safe_load(db_stream)
                    self._db_file = database
            else:
                raise FileNotFoundError(
                    "Provided database file path does not exist: {}".
                        format(self.database))
        elif isinstance(database, Mapping):
            self._database = database
        else:
            raise NotImplementedError(
                "'{}' is not a supported database type".
                    format(database.__class__.__name__))

    @property
    def database(self):
        """
        Database contents

        :return dict: contents of the database
        """
        return self._database

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
        if id in self.database:
            _LOGGER.warning("'{}' already in database.".format(id))
            if not overwrite:
                return False
        if id in self.cache:
            _LOGGER.warning("'{}' already in cache.".format(id))
            if not overwrite:
                return False
        self._cache[id] = value
        _LOGGER.info("Cached new record: {}={}({})".format(id, value, type))
        if not cache:
            self.write()
        return True

    def remove(self, id):
        """
        Remove a result

        :param str id: the name of the result to remove
        :return bool: whether the result was removed or not
        """
        removed = False
        if id in self.database:
            del self._database[id]
            removed = True
        if id in self.cache:
            del self._cache[id]
            removed = True
        if not removed:
            _LOGGER.warning("'{}' has not been reported".format(id))
        return removed

    def write(self):
        """
        Write reported results to the database
        """
        self._database.update(self._cache)
        if self._db_file:
            with open(self._db_file, "w") as db_stream:
                yaml.dump(self._database, db_stream, default_flow_style=False)
        _LOGGER.info("Wrote {} cached records: {}".
                     format(len(self.cache), self.cache))
        self._cache = {}


def main():
    """ Primary workflow """
    parser = logmuse.add_logging_options(build_argparser())
    args = parser.parse_args()
    global _LOGGER
    _LOGGER = logmuse.logger_via_cli(args, make_root=True)
    msg = "ID: {id}; type: {type}; value: {value}; database: {database}"
    _LOGGER.debug(msg.format(id=args.id, type=args.type, value=args.value,
                            database=args.database))
    db = {} if args.database is None else args.database
    psm = PipeStatManager(database=db, name="test")
    psm.report(id=args.id, type=args.type, value=args.value)