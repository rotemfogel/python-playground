from copy import deepcopy
from pprint import pprint
from typing import List

use_input_date = "use_input_date"
ignore_all_columns = "ignore_all_columns"


def get_adjusted_template(entries: dict, steps_template: List[dict]) -> List[dict]:
    new_steps_template = deepcopy(steps_template)
    step_args = new_steps_template[0]["HadoopJarStep"]["Args"]
    search_entries = [
        "input_path",
        "table",
        "db",
        "include_columns",
        ignore_all_columns,
        "ignore_data_types",
        use_input_date,
    ]
    keys = entries.keys()
    for element in search_entries:
        if element in keys:
            arg = element.replace("_", "-")
            if element == ignore_all_columns:
                step_args.extend([f"--{arg}"])
            elif element == use_input_date:
                # check: should append --input-date
                if entries.get(element, False):
                    step_args.extend([f"--{arg}"])
            else:
                step_args.extend([f"--{arg}", f"{{{{ params.{element} }}}}"])
    new_steps_template[0]["HadoopJarStep"]["Args"] = step_args
    return new_steps_template


if __name__ == "__main__":
    import yaml

    with open("data_statistics.yaml", "r") as conf_file:
        cluster_conf = yaml.safe_load(conf_file)
    template_steps = cluster_conf["steps"]

    with open("variables.yaml", "r") as f:
        variables = yaml.safe_load(f)

    data_stats = variables["data_statistics"]
    for entry in data_stats:
        print(entry["table"])
        pprint(template_steps[0]["HadoopJarStep"]["Args"])
        adjusted_template = get_adjusted_template(entry, template_steps)
        pprint(adjusted_template[0]["HadoopJarStep"]["Args"])
