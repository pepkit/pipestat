import sys
from logging import getLogger

import logmuse
from ubiquerg import expandpath

from .argparser import build_argparser
from .const import *
from .pipestat import PipestatManager

_LOGGER = getLogger(PKG_NAME)


def main():
    """ Primary workflow """
    from inspect import getdoc

    parser = logmuse.add_logging_options(build_argparser(getdoc(PipestatManager)))
    args = parser.parse_args()
    if args.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    global _LOGGER
    _LOGGER = logmuse.logger_via_cli(args, make_root=True)
    _LOGGER.debug("Args namespace:\n{}".format(args))
    if args.config and not args.schema:
        parser.error("the following arguments are required: -s/--schema")
    psm = PipestatManager(
        namespace=args.namespace,
        schema_path=args.schema,
        results_file_path=args.results_file,
        config=args.config,
        database_only=args.database_only,
        status_schema_path=args.status_schema,
        flag_file_dir=args.flag_dir,
    )
    if args.command == REPORT_CMD:
        value = args.value
        result_metadata = psm.schema[args.result_identifier]
        if (
            result_metadata[SCHEMA_TYPE_KEY]
            in [
                "object",
                "image",
                "file",
            ]
            and os.path.exists(expandpath(value))
        ):
            from json import load

            _LOGGER.info(
                f"Reading JSON file with object type value: {expandpath(value)}"
            )
            with open(expandpath(value), "r") as json_file:
                value = load(json_file)
        psm.report(
            record_identifier=args.record_identifier,
            values={args.result_identifier: value},
            force_overwrite=args.overwrite,
            strict_type=not args.try_convert,
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
