import argparse

from ubiquerg import VersionInHelpParser

from ._version import __version__
from .const import *


def _env_txt(arg_name):
    """
    Check if env var set and produce text
    """
    arg_val = os.environ.get(ENV_VARS[arg_name])
    txt = f"If not provided '{ENV_VARS[arg_name]}' env var will be used. "
    return txt + (
        "Currently not set" if arg_val is None else f"Currently set to: {arg_val}"
    )


def build_argparser(desc):
    """
    Builds argument parser.
    :param str desc: additional description to print in help
    :return argparse.ArgumentParser
    """
    banner = "%(prog)s - report pipeline results"
    additional_description = desc
    parser = VersionInHelpParser(
        version=__version__, description=banner, epilog=additional_description
    )

    subparsers = parser.add_subparsers(dest="command")

    def add_subparser(cmd, msg, subparsers):
        return subparsers.add_parser(
            cmd,
            description=msg,
            help=msg,
            formatter_class=lambda prog: argparse.HelpFormatter(
                prog, max_help_position=40, width=90
            ),
        )

    sps = {}
    # common arguments
    for cmd in SUBPARSER_MSGS.keys():
        sps[cmd] = add_subparser(cmd, SUBPARSER_MSGS[cmd], subparsers)
        # status is nested and status subcommands require config path
        if cmd == STATUS_CMD:
            continue
        sps[cmd].add_argument(
            "-n",
            "--namespace",
            type=str,
            metavar="N",
            help=f"Name of the pipeline to report result for. {_env_txt('namespace')}",
        )

    status_subparser = sps[STATUS_CMD]
    status_subparsers = status_subparser.add_subparsers(dest="subcommand")

    status_sps = {}
    for cmd, desc in STATUS_SUBPARSER_MESSAGES.items():
        status_sps[cmd] = add_subparser(cmd, desc, status_subparsers)
        status_sps[cmd].add_argument(
            "-n",
            "--namespace",
            type=str,
            metavar="N",
            help=f"Name of the pipeline to report result for. {_env_txt('namespace')}",
        )
        if cmd == STATUS_SET_CMD:
            status_sps[cmd].add_argument(
                "-i",
                "--status-identifier",
                metavar="S",
                help="Status identifier to use",
                required=True,
            )
        status_sps[cmd].add_argument(
            "-f",
            "--results-file",
            type=str,
            metavar="F",
            help=f"Path to the YAML file where the results will be stored. "
            f"This file will be used as {PKG_NAME} backend and to restore"
            f" the reported results across sessions",
        )
        status_sps[cmd].add_argument(
            "-c",
            "--config",
            type=str,
            metavar="C",
            help=f"Path to the YAML configuration file. {_env_txt('config')}",
        )
        status_sps[cmd].add_argument(
            "-a",
            "--database-only",
            action="store_true",
            help="Whether the reported data should not be stored in the memory,"
            " only in the database.",
        )
        status_sps[cmd].add_argument(
            "-s",
            "--schema",
            type=str,
            metavar="S",
            help=f"Path to the schema that defines the results that can be reported. {_env_txt('schema')}",
        )
        status_sps[cmd].add_argument(
            "--status-schema",
            type=str,
            metavar="ST",
            help=f"Path to the status schema. "
            f"Default will be used if not provided: {STATUS_SCHEMA}",
        )
        status_sps[cmd].add_argument(
            "--flag-dir",
            type=str,
            metavar="FD",
            help=f"Path to the flag directory in case YAML file is "
            f"the pipestat backend.",
        )
        status_sps[cmd].add_argument(
            "-r",
            "--record-identifier",
            type=str,
            metavar="R",
            help=f"ID of the record to report the result for. {_env_txt('record_identifier')}",
        )

    # remove, report and inspect
    for cmd in [REMOVE_CMD, REPORT_CMD, INSPECT_CMD, RETRIEVE_CMD]:
        sps[cmd].add_argument(
            "-f",
            "--results-file",
            type=str,
            metavar="F",
            help=f"Path to the YAML file where the results will be stored. "
            f"This file will be used as {PKG_NAME} backend and to restore"
            f" the reported results across sessions",
        )
        sps[cmd].add_argument(
            "-c",
            "--config",
            type=str,
            metavar="C",
            help=f"Path to the YAML configuration file. {_env_txt('config')}",
        )
        sps[cmd].add_argument(
            "-a",
            "--database-only",
            action="store_true",
            help="Whether the reported data should not be stored in the memory,"
            " only in the database.",
        )
        sps[cmd].add_argument(
            "-s",
            "--schema",
            type=str,
            metavar="S",
            help=f"Path to the schema that defines the results that can be reported. {_env_txt('schema')}",
        )
        sps[cmd].add_argument(
            "--status-schema",
            type=str,
            metavar="ST",
            help=f"Path to the status schema. "
            f"Default will be used if not provided: {STATUS_SCHEMA}",
        )
        sps[cmd].add_argument(
            "--flag-dir",
            type=str,
            metavar="FD",
            help=f"Path to the flag directory in case YAML file is "
            f"the pipestat backend.",
        )

    # remove and report
    for cmd in [REMOVE_CMD, REPORT_CMD, RETRIEVE_CMD]:
        sps[cmd].add_argument(
            "-i",
            "--result-identifier",
            required=True,
            type=str,
            metavar="I",
            help="ID of the result to report; needs to be defined in the schema",
        )
        sps[cmd].add_argument(
            "-r",
            "--record-identifier",
            type=str,
            metavar="R",
            help=f"ID of the record to report the result for. {_env_txt('record_identifier')}",
        )

    # report
    sps[REPORT_CMD].add_argument(
        "-v",
        "--value",
        required=True,
        metavar="V",
        help="Value of the result to report",
    )

    sps[REPORT_CMD].add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="Whether the result should override existing ones in "
        "case of name clashes",
    )

    sps[REPORT_CMD].add_argument(
        "-t",
        "--try-convert",
        action="store_true",
        help="Whether to try to convert the reported value into reqiuired "
        "class in case it does not meet the schema requirements",
    )

    # inspect
    sps[INSPECT_CMD].add_argument(
        "-d", "--data", action="store_true", help="Whether to display the data"
    )

    return parser
