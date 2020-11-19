#!/usr/bin/env python3
import json
import argparse

# Check Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="""Input path to the JSON file. To Download the file use:
    `bq show --schema  --format=prettyjson fulfillment-dwh-staging:cl.audit_logs &> bqSchema.json`""", required=True)
parser.add_argument("--output", help="Output path of the markdown template.", required=True)
parser.add_argument("--table-name", help="Name of the table.", required=True)  # Tmp until this connects directly to BQ
args = parser.parse_args()

input_path = args.input
output_path = args.output
table_name = args.table_name

# Passing the path to where the JSON exists. (next step: read it from BQ directly
file = open(input_path, 'r')
read_File = file.read()

# Variables
headers = """| Column | Type | Description |
| :--- | :--- | :--- |
"""
schema_dictionary = dict()
fields_levels = dict()
parsed_json = json.loads(read_File)


def unmarshal(bq_json, index, level_name, level):
    if index < len(bq_json):
        obj = bq_json[index]
        json_name = obj['name']
        json_name_value = obj['name']
        json_type = obj['type']
        if 'fields' in obj:
            json_field = obj['fields']
            json_name_value = "[{}](#{})".format(json_name, json_name.replace("_", "-"))
            unmarshal(json_field, 0, json_name, level + 1)

        new_line = "| {} | `{}` | N/A. |".format(json_name_value, json_type)
        schema_dictionary.setdefault(level_name, []).append(new_line)
        fields_levels.setdefault(level_name, level)
        unmarshal(bq_json, index + 1, level_name, level)


# Call unmarshal
unmarshal(parsed_json, 0, table_name, 1)

# Write to file
write_file = open(output_path, "w+")

for key, values in schema_dictionary.items():
    write_file.write("#" * fields_levels[key] + " " + key.replace("_", " ").title() + "\n\n")
    write_file.write(headers)
    write_file.write("\n".join(values))
    write_file.write("\n\n")

write_file.close()
file.close()
