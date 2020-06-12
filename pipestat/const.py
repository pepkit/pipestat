from collections import Mapping

LOCK_PREFIX = "lock."
REPORT_CMD = "report"
SUBPARSER_MSGS = {
    REPORT_CMD: "Report a result."
}
LIBS_BY_BACKEND = {"mongo": ["pymongo", "mongodict"]}
CLASSES_BY_TYPE = {"integer": int, "float": float, "string": str,
                   "boolean": bool, "object": Mapping, "null": type(None),
                   "array": list, "file": str, "image": str}
