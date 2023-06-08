import sys
from logging import getLogger

import logmuse
from ubiquerg import expandpath

from .argparser import (
    build_argparser,
    REPORT_CMD,
    INSPECT_CMD,
    REMOVE_CMD,
    RETRIEVE_CMD,
    STATUS_CMD,
)
from .const import *
from .exceptions import SchemaNotFoundError
from .pipestat import PipestatManager

_LOGGER = getLogger(PKG_NAME)


def main():
    """Primary workflow"""
    from inspect import getdoc

    parser = logmuse.add_logging_options(build_argparser(getdoc(PipestatManager)))
    args = parser.parse_args()
    if args.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    global _LOGGER
    _LOGGER = logmuse.logger_via_cli(args, make_root=True)
    _LOGGER.debug("Args namespace:\n{}".format(args))
    if args.config and not args.schema and args.command != STATUS_CMD:
        parser.error("the following arguments are required: -s/--schema")
    psm = PipestatManager(
        # namespace=args.namespace
        schema_path=args.schema,
        results_file_path=args.results_file,
        config_file=args.config,
        database_only=args.database_only,
        # status_schema_path=args.status_schema,
        flag_file_dir=args.flag_dir,
    )
    types_to_read_from_json = ["object"] + list(CANONICAL_TYPES.keys())
    if args.command == REPORT_CMD:
        value = args.value
        if psm.schema is None:
            raise SchemaNotFoundError(msg="report", cli=True)
        result_metadata = psm.schema.results_data[args.result_identifier]
        if result_metadata[SCHEMA_TYPE_KEY] in types_to_read_from_json:
            path_to_read = expandpath(value)
            if os.path.exists(path_to_read):
                from json import load

                _LOGGER.info(f"Reading JSON file: {path_to_read}")
                with open(path_to_read, "r") as json_file:
                    value = load(json_file)
            else:
                _LOGGER.info(f"Path to read for {value} doesn't exist: {path_to_read}")
        psm.report(
            record_identifier=args.record_identifier,
            values={args.result_identifier: value},
            force_overwrite=args.overwrite,
            strict_type=args.skip_convert,
        )
    if args.command == INSPECT_CMD:
        print("\n")
        print(psm)
        if args.data and not args.database_only:
            print("\nData:")
            print(psm.data)
    if args.command == REMOVE_CMD:
        psm.remove(
            result_identifier=args.result_identifier,
            record_identifier=args.record_identifier,
        )
    if args.command == RETRIEVE_CMD:
        print(
            psm.retrieve(
                result_identifier=args.result_identifier,
                record_identifier=args.record_identifier,
            )
        )
    if args.command == STATUS_CMD:
        if args.subcommand == STATUS_GET_CMD:
            print(psm.get_status(record_identifier=args.record_identifier))
        if args.subcommand == STATUS_SET_CMD:
            psm.set_status(
                status_identifier=args.status_identifier,
                record_identifier=args.record_identifier,
            )
    sys.exit(0)
