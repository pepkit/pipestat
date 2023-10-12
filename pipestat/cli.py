import sys
import os
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
    STATUS_GET_CMD,
    STATUS_SET_CMD,
    INIT_CMD,
    SUMMARIZE_CMD,
    SERVE_CMD,
    LINK_CMD,
)
from .const import *
from .exceptions import SchemaNotFoundError, PipestatStartupError
from .pipestat import PipestatManager
from .helpers import init_generic_config
from pipestatreader import main as readermain

_LOGGER = getLogger(PKG_NAME)


def main(test_args=None):
    """Primary workflow"""
    from inspect import getdoc

    parser = logmuse.add_logging_options(build_argparser(getdoc(PipestatManager)))
    if test_args:
        args = parser.parse_args(test_args)
    else:
        args = parser.parse_args()
    if args.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    global _LOGGER
    _LOGGER = logmuse.logger_via_cli(args, make_root=True)
    _LOGGER.debug("Args namespace:\n{}".format(args))
    if args.command == INIT_CMD:
        sys.exit(int(not init_generic_config()))
    # if args.config and not args.schema and args.command != STATUS_CMD:
    #     parser.error("the following arguments are required: -s/--schema")
    if not args.config and not args.results_file:
        msg = (
            "Either a config file or a results file must be provided. Either must be supplied to the object "
            "constructor or via environment variable. \nPlease see: http://pipestat.databio.org/en/dev/cli/"
        )
        raise PipestatStartupError(msg)

    if args.command == SUMMARIZE_CMD:
        psm = PipestatManager(
            schema_path=args.schema,
            results_file_path=args.results_file,
            config_file=args.config,
            pipeline_type=args.pipeline_type,
        )
        results_path = args.config or args.results_file
        html_report_path = psm.summarize()
        _LOGGER.info(f"\nGenerating HTML Report from {results_path} at: {html_report_path}\n")

        sys.exit(0)

    if args.command == LINK_CMD:
        psm = PipestatManager(
            schema_path=args.schema,
            results_file_path=args.results_file,
            config_file=args.config,
        )
        linkdir = psm.link(link_dir=args.link_dir)
        _LOGGER.info(f"\nGenerating symlink directory at: {linkdir}\n")
        sys.exit(0)

    if args.command == SERVE_CMD:
        readermain(configfile=args.config, host=args.host, port=args.port)
        sys.exit(0)

    psm = PipestatManager(
        schema_path=args.schema,
        results_file_path=args.results_file,
        config_file=args.config,
        database_only=args.database_only,
        flag_file_dir=args.flag_dir,
        pipeline_type=args.pipeline_type,
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

        reported_results = psm.report(
            record_identifier=args.record_identifier,
            values={args.result_identifier: value},
            force_overwrite=args.overwrite,
            strict_type=args.skip_convert,
        )
        if reported_results is not False:
            for r in reported_results:
                print(r)
    if args.command == INSPECT_CMD:
        print("\n")
        print(psm)
        if args.data and not args.database_only:
            print("\nData:")
            print(psm.backend._data)
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
