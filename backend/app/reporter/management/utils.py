import hashlib
import json
from copy import deepcopy
from pprint import pprint


def hash_dict(entry):
    json_str = json.dumps(entry, sort_keys=True)
    return hashlib.sha256(
        json_str.encode("utf-8")
    ).hexdigest()


def dict_diff(dict_1, dict_2, prefix = None):
    diff = []
    keys = set(dict_1.keys()).union(dict_2.keys())
    for k  in keys:
        val_1 = dict_1.get(k)
        val_2 = dict_2.get(k)
        if isinstance(val_1, dict) and isinstance(val_2, dict):
            diff.extend(
                dict_diff(
                    val_1,
                    val_2,
                    prefix = k
                )
            )

        elif isinstance(val_1, dict) != isinstance(val_2, dict):
            # Error out if type dict type change
            raise ValueError(
                "Currently, a switch from or to a dict is not supported"
                " in the `dict_diff` code."
            )


        elif val_1 != val_2:
            field = "__".join([prefix, k]) if prefix else k
            diff.append(
                {
                    "field": field,
                    "old_value": val_1,
                    "new_value": val_2
                }
            )

    return diff


def get_fain(record):
    project_num_split = record.get("project_num_split")
    if project_num_split is None:
        return

    return "".join(
        [
            project_num_split["activity_code"],
            project_num_split["ic_code"],
            project_num_split["serial_num"]
        ]
    )
