from typing import List


def get_adjusted_template(entries: dict, steps_template: List[dict]) -> List[dict]:
    step_args = steps_template[0]['HadoopJarStep']['Args']
    search_entries = ['input_path', 'table', 'db', 'include_columns', 'ignore_data_types']
    keys = entries.keys()
    for element in search_entries:
        if element in keys:
            step_args.extend([f'--{element.replace("_", "-")}', f'{{{{ params.{element} }}}}'])
    steps_template[0]['HadoopJarStep']['Args'] = step_args
    return steps_template


if __name__ == "__main__":
    import yaml

    with open('data_statistics.yaml', 'r') as conf_file:
        cluster_conf = yaml.safe_load(conf_file)
    template_steps = cluster_conf['steps']

    with open('variables.yaml', 'r') as f:
        variables = yaml.safe_load(f)

    data_stats = variables['data_statistics']
    for entry in data_stats:
        print(f"before:\n{template_steps[0]['HadoopJarStep']['Args']}")
        adjusted_template = get_adjusted_template(entry, template_steps)
        print(f"after:\n{adjusted_template[0]['HadoopJarStep']['Args']}")
