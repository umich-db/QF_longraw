'''
Convert DuckDB EXPLAIN ANALYZE json files to a single csv file with [id, json] 
'''

import os
import json
import csv
import argparse
import re

# Argument parsing
parser = argparse.ArgumentParser(description="Convert DuckDB JSON plans to PostgreSQL-like format.")
parser.add_argument('--input_folder', type=str, required=True, help='Input folder containing DuckDB plan JSON files')
parser.add_argument('--output_csv_path', type=str, required=True, help='Output CSV file path')
parser.add_argument('--template_start', type=int, default=1, help='Start of template range (inclusive)')
parser.add_argument('--template_end', type=int, default=23, help='End of template range (exclusive)')
parser.add_argument('--query_start', type=int, default=1, help='Start of query range (inclusive)')
parser.add_argument('--query_end', type=int, default=101, help='End of query range (exclusive)')
parser.add_argument('--map_operator_type', type=bool, default=False, help='Whether to map DuckDB operator types to PostgreSQL equivalents')
args = parser.parse_args()

OPERATOR_TYPE_MAPPING = {
    "PROJECTION": "Projection",
    "ORDER_BY": "Sort",
    "PERFECT_HASH_GROUP_BY": "Aggregate",
    "HASH_GROUP_BY": "Aggregate",
    "SEQ_SCAN": "Seq Scan",
    "TABLE_SCAN": "Seq Scan",
    "CTE_SCAN": "CTE Scan",
    "HASH_JOIN": "Hash Join",
    "RIGHT_DELIM_JOIN": "Hash Join",
    "MERGE_JOIN": "Merge Join",
    "TOP_N": "Limit",
    "FILTER": "Filter",
}

def normalize_filter_expr(expr):
    parts = re.split(r'\s+(AND|OR)\s+', expr, flags=re.IGNORECASE)
    wrapped_parts = []
    for part in parts:
        part_strip = part.strip()
        if part_strip.upper() in {'AND', 'OR'}:
            wrapped_parts.append(f' {part_strip.upper()} ')
        elif part_strip:
            part_cleaned = re.sub(r'::[A-Z]+', lambda m: m.group(0).lower(), part_strip)
            part_cleaned = re.sub(r'(?<!\s)(<=|>=|!=|=|<|>)(?!\s)', r' \1 ', part_cleaned)
            wrapped_parts.append(f'({part_cleaned})')
    return ''.join(wrapped_parts)

def find_relation_name_recursive(node):
    extra = node.get("extra_info", {})
    for key in ["Table", "Text"]:
        if key in extra:
            return extra[key]
    for child in node.get("children", []):
        result = find_relation_name_recursive(child)
        if result:
            return result
    return None

def convert_operator(node):
    raw_type = node.get("operator_type", "Unknown")
    operator_type = OPERATOR_TYPE_MAPPING.get(raw_type, raw_type) if args.map_operator_type else raw_type

    if operator_type == "Filter":
        if "children" in node and len(node["children"]) == 1:
            filter_expr = node.get("extra_info", {}).get("Expression") or node.get("extra_info", {}).get("Filters")
            child_node = node["children"][0]
            merged = convert_operator(child_node)
            if filter_expr:
                if isinstance(filter_expr, str):
                    merged["Filter"] = normalize_filter_expr(filter_expr)
                elif isinstance(filter_expr, list):
                    subfilters = [normalize_filter_expr(f) for f in filter_expr]
                    merged["Filter"] = f'({" AND ".join(subfilters)})'
                else:
                    merged["Filter"] = str(filter_expr)

            merged["Actual Rows"] = node.get("operator_cardinality", merged.get("Actual Rows", 0))
            merged["Actual Startup Time"] = round(node.get("operator_timing", 0) * 1000, 3)
            merged["Actual Total Time"] = round(node.get("operator_timing", 0) * 1000, 3)
            merged["Actual Loops"] = 1
            return merged
        else:
            print(f"Warning: Filter node has != 1 child")

    converted = {
        "Node Type": operator_type,
        "Actual Rows": node.get("operator_cardinality", 0),
        "Actual Loops": 1,
        "Actual Startup Time": round(node.get("operator_timing", 0) * 1000, 3),
        "Actual Total Time": round(node.get("operator_timing", 0) * 1000, 3),
    }

    extra = node.get("extra_info", {})
    relation_name = extra.get("Table", None) or find_relation_name_recursive(node)
    if relation_name:
        converted["Relation Name"] = relation_name

    if "Projections" in extra:
        converted["Output"] = extra["Projections"]
    if "Order By" in extra:
        converted["Sort Key"] = extra["Order By"]
    if "Filters" in extra:
        raw_filter = extra["Filters"]
        if isinstance(raw_filter, str):
            converted["Filter"] = normalize_filter_expr(raw_filter)
        elif isinstance(raw_filter, list):
            converted["Filter"] = f'({" AND ".join([normalize_filter_expr(f) for f in raw_filter])})'
        else:
            converted["Filter"] = str(raw_filter)
    if "Groups" in extra:
        converted["Group Key"] = extra["Groups"]
    if "Aggregates" in extra:
        agg = extra["Aggregates"]
        converted["Output"] = converted.get("Output", []) + ([agg] if isinstance(agg, str) else agg)
    if operator_type == "Aggregate":
        converted["Strategy"] = "Hashed"

    if "children" in node:
        converted["Plans"] = [convert_operator(child) for child in node["children"]]

    return converted

# Main conversion loop
output_rows = []
id_counter = 1
empty_files = []

for template in range(args.template_start, args.template_end):
    # if args.database == "tpch" and template == 15:
    #     continue
    for query in range(args.query_start, args.query_end):
        filename = f"{template}_{query}_run0.json"
        filepath = os.path.join(args.input_folder, filename)
        if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
            continue
        with open(filepath, 'r') as f:
            duckdb_data = json.load(f)

        plan_node = convert_operator(duckdb_data["children"][0])
        plan_node["Actual Rows"] = duckdb_data.get("rows_returned", 0)
        plan_node["Actual Loops"] = 1
        converted_json = {
            "Plan": plan_node,
            "Execution Time": round(duckdb_data.get("latency", 0) * 1000, 3)
        }
        output_rows.append({
            "id": id_counter,
            "json": json.dumps(converted_json)
        })
        id_counter += 1
        break

# Output CSV
output_dir = os.path.dirname(args.output_csv_path)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)

with open(args.output_csv_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['id', 'json'])
    writer.writeheader()
    for row in output_rows:
        writer.writerow(row)
