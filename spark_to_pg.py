'''
Convert Spark EXPLAIN ANALYZE txt files to a single csv file with [id, json] 
'''

import traceback
import os
import sys
import json
from collections import deque
from copy import deepcopy
import re
import shutil

from parsers.spark_node import Node
import parsers.utils_ms as utils_ms

CHILDREN_KEY = "children"
DATA_KEY = "data"
OPERATOR_KEY = "operator"
COST_KEY = "estimate_cost_start"
ROWS_KEY = "estimate_rows"
ACTUAL_TIME_START_KEY = "actual_time_start"
ACTUAL_TIME_END_KEY = "actual_time_end"
OP_TIME_START_KEY = "operator_time_start"
OP_TIME_END_KEY = "operator_time_end"
OP_TIME_KEY = "operator_time"
ACTUAL_ROWS_KEY = "actual_rows"
ACTUAL_LOOPS_KEY = "actual_loops"
TRAINABLE_KEY = "trainable"
EXE_KEY = "executed"
NOT_EXE_KEY = "not_executed"

def reduce_opname(opname, keywords=[":", " on ", " using ", r" #\d+[a-zA-Z]", " with deduplication", r" \("]):
    # algorithm: only keep contents before all the keywords
    pattern = r'({})'.format('|'.join(keyword for keyword in keywords))
    # Search for the pattern in the text
    match = re.search(pattern, opname)
    if match:
        return opname[:match.start()].strip()
    else:
        return opname  # Return the original text if no keywords are found

def extract_target_table(opname):
    # Find "on <table name>" in the operator
    match = re.search(r' on ([\w_]+)', opname)
    return match.group(1) if match else None

def update_looped_values(plan_node):
    for subplan in plan_node.children:
        update_looped_values(subplan)
    for key in [ACTUAL_ROWS_KEY, ROWS_KEY]:
        if plan_node.extracted_data.get(key) is not None:
            plan_node.extracted_data[key] *= plan_node.extracted_data[ACTUAL_LOOPS_KEY]

def update_operator_runtime(plan_node):
    #^ post-order traversal 
    # process the children
    for subplan in plan_node.children:
        update_operator_runtime(plan_node=subplan)

    # process the root node
    # calculate startup times and total time
    op_name = reduce_opname(plan_node.extracted_data[OPERATOR_KEY])
    table = extract_target_table(plan_node.extracted_data[OPERATOR_KEY])

    # for not-executed nodes
    if "never executed" in plan_node.extracted_data[OPERATOR_KEY]:
        plan_node.extracted_data[OP_TIME_KEY] = 0.0
        plan_node.extracted_data[OP_TIME_START_KEY] = 0.0
        subplan_total_time = sum(
            subplan.extracted_data.get(OP_TIME_END_KEY, 0.0) or 0.0
            for subplan in plan_node.children
        )
        plan_node.extracted_data[OP_TIME_END_KEY] = subplan_total_time
        return

    # for executed nodes
    try:
        actual_time_start = float(plan_node.extracted_data.get(ACTUAL_TIME_START_KEY, 0.0) or 0.0)
        actual_time_end = float(plan_node.extracted_data.get(ACTUAL_TIME_END_KEY, 0.0) or 0.0)
        loops = float(plan_node.extracted_data.get(ACTUAL_LOOPS_KEY, 1.0) or 1.0)

        # Calculate operator time considering loops
        plan_node.extracted_data[OP_TIME_START_KEY] = actual_time_start * loops
        plan_node.extracted_data[OP_TIME_END_KEY] = actual_time_end * loops

        subplan_total_time = sum([subplan.extracted_data.get(OP_TIME_END_KEY, 0.0) or 0.0 for subplan in plan_node.children])

        # Calculate operator's execution time (subtract child execution time from total)
        plan_node.extracted_data[OP_TIME_KEY] = max(0.0, plan_node.extracted_data[OP_TIME_END_KEY] - subplan_total_time)

        # Update operator name and table name
        plan_node.extracted_data[OPERATOR_KEY] = op_name
        if table is not None:
            plan_node.extracted_data[utils_ms.TABLE_NAME_KEY] = table

    except Exception as e:
        print("ERROR in update_operator_runtime:")
        print(f"  Plan Node: {plan_node.data}")
        print(f"  Extracted Data: {plan_node.extracted_data}")
        traceback.print_exc()  # Prints the traceback with line number
        raise  # re-raise if you want it to crash, or comment this to continue silently

def update_input_rows(plan_node):
    for subplan in plan_node.children:
        update_input_rows(plan_node=subplan)
    # 1. for leaf node, load the table row count from prepared external file
    # 2. for non-leaf node, pick the maximum estimate row count among children
    if len(plan_node.children) == 0:  # is leaf node
        plan_node.extracted_data[utils_ms.INPUT_ROWS_KEY] = float(
            plan_node.extracted_data.get(ROWS_KEY, 0.0)
        ) if plan_node.extracted_data.get(ROWS_KEY) is not None else 0.0

    else: # non-leaf
        try:
            estimate_rows = lambda subplan: subplan.extracted_data[ROWS_KEY] if ROWS_KEY in subplan.extracted_data else subplan.extracted_data[utils_ms.INPUT_ROWS_KEY]
            to_float = lambda x: float(x) if x is not None else 0.0
            plan_node.extracted_data[utils_ms.INPUT_ROWS_KEY] = sum([to_float(estimate_rows(subplan)) for subplan in plan_node.children])
        except Exception as e:
            # print(plan_node)
            raise e

def update_operator_cost(plan_node):
    for subplan in plan_node.children:
        update_operator_cost(subplan)
    # calculate the operator cost
    if utils_ms.COST_KEY in plan_node.extracted_data and isinstance(plan_node.extracted_data[utils_ms.COST_KEY], float):
        null_to_zero = lambda x: x if x is not None else 0.0
        plan_node.extracted_data[utils_ms.OP_COST_KEY] = plan_node.extracted_data[utils_ms.COST_KEY] - sum([null_to_zero(subplan.extracted_data.get(utils_ms.COST_KEY, 0)) for subplan in plan_node.children])


class PlanParser():
    def __init__(self, op_categories):
        self.op_categories_ = op_categories

    def parse_one_plan(self, plan_chuck: str):
        def merge_stack(stack, stop_indent_level):
            '''Merge an incremental stack of (node, indent) pairs'''
            children_buffer = []
            print(f"\n merge_stack called with stop_indent_level={stop_indent_level}")
            print(" Current stack BEFORE merging:")
            for i, (node, indent) in enumerate(stack):
                print(f"  [{i}] {node.extracted_data[OPERATOR_KEY]} (indent={indent})")

            while len(stack) >= 2 and stack[-1][1] > stop_indent_level:
                top = stack.pop()
                children_buffer.insert(0, top[0])
                print(f"  Merging: {top[0].extracted_data[OPERATOR_KEY]} (Indent Level: {top[1]})")

                if stack[-1][1] > top[1]:
                    raise ValueError("children node cannot have smaller indentation!")
                elif stack[-1][1] == top[1]:
                    continue
                else:  # parent
                    stack[-1][0].children = deepcopy(children_buffer)
                    children_buffer.clear()

            print(" Current stack AFTER merging:")
            for i, (node, indent) in enumerate(stack):
                print(f" {node.extracted_data[OPERATOR_KEY]} (indent={indent})")
            print("")

        '''Parse a single plan chunk'''

        lines = plan_chuck.explain_analyze_output.replace('\\n', '\n').splitlines()

        print(f"Line 208, lines: {lines}")
        # If the first line does not start with indentation or '+-', treat it as the root
        if lines and not lines[0].lstrip().startswith("+-") and not lines[0].startswith(" "):
            root_line = lines[0]
            lines[0] = "+- " + root_line  # artificially mark as plan root with indent token
            print(f"lines[0]:{lines[0]}")
        
        print(f"\n==== DEBUG: Raw plan lines ====")
        for i, line in enumerate(lines):
            print(f"[{i}] {line}")
        print("==== End of plan lines ====\n")

        stack = deque()

        
        for i, line in enumerate(lines):
            line = line.rstrip()
            if line == "" or "EXPLAIN" in line:
                continue  # Skip empty or header lines

            # Count indentation by leading spaces before '+-'
            # leading_spaces = len(line) - len(line.lstrip(' '))
            # this_indent_level = leading_spaces // 2  # Adjust if your indent spacing differs

            # For Spark: Strip '+-' prefix and count the leading spaces
            # Count indentation level based on position of '+-'
            original_line = line
            prefix_match = re.search(r'^(\s*)(\+|-|:|:-)', original_line)

            if prefix_match:
                prefix = prefix_match.group(1)
                # Assume each level is 2 spaces (or '  ')
                this_indent_level = len(prefix) // 2
            else:
                this_indent_level = 0  # root-level operator

            # Strip the '+-' and leading spaces for clean operator line
            line = re.sub(r'^\s*[:\+\-=\s]+', '', line)

            print(f"Processing line [{i}] (indent={this_indent_level}): {line}")

            # Extract operator name and details
            op_match = re.match(r'([A-Za-z0-9_]+)\s*(.*)', line)
            if op_match:
                op_name = op_match.group(1)
                op_details = op_match.group(2).strip()
            else:
                op_name = line
                op_details = ""

            print(f"\n  -> Extracted op_name: '{op_name}', op_details: '{op_details}'")

            # Create Node
            print("DEBUG: Before Node creation")
            this_node = Node(line.strip())
            print("DEBUG: After Node creation")

            this_node.extracted_data[OPERATOR_KEY] = op_name
            this_node.extracted_data['op_details'] = op_details

            # Merge if necessary
            print("\nCurrent Stack:")
            for idx, (node, indent) in enumerate(stack):
                print(f"  Stack[{idx}] -> Node: {node.extracted_data[OPERATOR_KEY]} (Indent Level: {indent})")

            if len(stack) > 0:
                prev_indent_level = stack[-1][1]
                if prev_indent_level > this_indent_level:
                    print("\n==== PARSE_ONE_PLAN DEBUG ====")
                    print(f"Current line: '{line}'")
                    print(f"  This indent level: {this_indent_level}")
                    print(f"  Stack before merge_stack: {[ (n.extracted_data[OPERATOR_KEY], ind) for n, ind in stack ]}")

                    merge_stack(stack, this_indent_level)

                    if not stack:
                        raise ValueError(f"Stack unexpectedly empty after merging! Line: '{line}' (indent={this_indent_level})")

                stack.append((this_node, this_indent_level))
            else:
                stack.append((this_node, this_indent_level))

            print("Updated Stack After Line:")
            for idx, (node, indent) in enumerate(stack):
                print(f"  Stack[{idx}] -> Node: {node.extracted_data[OPERATOR_KEY]} (Indent Level: {indent})")
            print("\n")

        # Final merge for remaining stack
        merge_stack(stack, 0)

        if len(stack) >= 2:
            raise ValueError("length of output stack shouldn't be more than 1!")
        elif len(stack) == 0:
            raise ValueError("Empty stack! Impossible unless the file is empty")

        update_looped_values(stack[0][0])
        update_operator_runtime(stack[0][0])
        update_input_rows(stack[0][0])
        update_operator_cost(stack[0][0])

        return stack[0][0]

class PlanChuck:
    def __init__(self, raw_string):
        lines = raw_string.strip().splitlines()
        self.root_line = None

        if lines and not lines[0].lstrip().startswith("+-") and not lines[0].startswith(" "):
            self.root_line = lines[0]
            lines[0] = "+- " + self.root_line  # Mark root

            # Manually indent the first child (if it exists)
            for i in range(1, len(lines)):
                if lines[i].lstrip().startswith("+-"):
                    lines[i] = "  " + lines[i]
                    # print(f"{lines}")
        
        self.tree_lines = lines
        self.explain_analyze_output = "\n".join(self.tree_lines)

        # Debug output (optional)
        print(f"\n[PlanChuck] Final tree lines:")
        for i, line in enumerate(self.tree_lines):
            print(f"[{i}] {line}")

           
def parse_spark_stats_block(stats_text):
    """
    Parses the StatsOutput block from Spark and returns a list of dicts.
    """
    stats_list = []
    pattern = re.compile(r'(\w+)\s+estimated row count: (\w+), size in bytes: (\d+)')
    for line in stats_text.strip().splitlines():
        match = pattern.search(line)
        if match:
            node_type = match.group(1)
            row_count = match.group(2)
            size_bytes = int(match.group(3))
            plan_rows = None if row_count == "unknown" else int(row_count)
            stats_list.append({
                "node_type": node_type,
                "plan_rows": plan_rows,
                "estimated_size_bytes": size_bytes
            })
    return stats_list

class SparkExplainParser():
    def __init__(self):
        self.raw_plan_list = []      # List of MysqlPlanChuck, each holds raw plan text
        self.parsed_plan_list = []   # List of parsed plan trees (Node objects)
        self.stats_list = []         # List of dicts: {'node_type': ..., 'plan_rows': ..., 'estimated_size_bytes': ...}
        self.execution_time = 0.0

        # Initialize the Spark-specific plan parser
        self.plan_parser = PlanParser(op_categories=None)  # op_categories not needed for Spark

    def load_raw_plans(self, filename):
        """
        For Spark: splits file into header (time cost), plan tree, and statsOutput.
        Stores the plan tree text and stats info.
        """
        with open(filename, 'r') as infile:
            content = infile.read()

        # Split into plan and stats sections
        parts = content.split("statsOutput:")
        if len(parts) != 2:
            raise ValueError(f"File {filename} does not contain 'statsOutput' section.")

        plan_part, stats_part = parts
        plan_lines = plan_part.strip().splitlines()
                
        # Find where the plan tree starts (the first line with "+-")
        tree_start_idx = None
        for idx, line in enumerate(plan_lines):
            if line.lstrip().startswith("+-"):
                tree_start_idx = idx
                break

        if tree_start_idx is None:
            raise ValueError(f"Cannot find a plan tree (lines starting with '+-') in file {filename}")

        if tree_start_idx > 0:
            root_line = plan_lines[tree_start_idx - 1]
            # print(f"root_line:{root_line}")
            plan_tree_text = root_line + '\n' + '\n'.join(plan_lines[tree_start_idx:])
            # print(f"\n On line 433, plan_tree_text:{plan_tree_text}")
        else:
            plan_tree_text = '\n'.join(plan_lines[tree_start_idx:])
            
        # plan_tree_text = '\n'.join(plan_lines[tree_start_idx:])
        print(f"In load_raw_plans, plan_tree_text:{plan_tree_text}")
        self.raw_plan_list = [PlanChuck(plan_tree_text)]
        self.stats_list = parse_spark_stats_block(stats_part.strip())
        self.execution_time = extract_total_execution_time_spark(filename)

    def parse_raw_plans(self):
        """
        Parses the raw plan text into tree structures and attaches stats.
        """
        if len(self.raw_plan_list) == 0:
            print("Plan list is empty! Load the plan from file first!")
            return

        self.parsed_plan_list = []

        for plan in self.raw_plan_list:
            print(f"In parse_raw_plan(), plan:{plan}")
            # Parse the plan text into a tree of Nodes
            plan_node = self.plan_parser.parse_one_plan(plan)

            # Attach stats (Plan Rows, Estimated Size) to nodes
            attach_stats_to_plan(plan_node, self.stats_list)

            self.parsed_plan_list.append(plan_node)

# Helper function: attaches stats to plan nodes
def attach_stats_to_plan(plan_node, stats_list):
    """
    Traverses the plan tree in pre-order and attaches 'Plan Rows' and 'Estimated Size' from Spark stats.
    Ensures one-to-one mapping between nodes and stats.
    """
    def flatten_preorder(node):
        result = [node]
        for child in node.children:
            result.extend(flatten_preorder(child))
        return result

    # Flatten the plan tree
    flat_nodes = flatten_preorder(plan_node)

    if len(flat_nodes) != len(stats_list):
        print(f"Warning: Node count ({len(flat_nodes)}) != stats count ({len(stats_list)}). Proceeding with min(len(flat_nodes), len(stats_list)).")

    for node, stats in zip(flat_nodes, stats_list):
        node.extracted_data['plan_rows'] = stats['plan_rows']
        node.extracted_data['estimated_size_bytes'] = stats['estimated_size_bytes']
        node.extracted_data['input_rows'] = stats['plan_rows'] if stats['plan_rows'] is not None else 1.0

    # Check if any stats are left
    remaining_stats = len(stats_list) - len(flat_nodes)
    if remaining_stats > 0:
        print(f"Warning: {remaining_stats} stats entries were not used.")
    elif len(flat_nodes) > len(stats_list):
        print(f"Warning: {len(flat_nodes) - len(stats_list)} plan nodes did not get stats assigned.")



def extract_total_execution_time_spark(filepath):
    """
    Extracts total execution time from Spark query plan file.
    Example line: 'time cost: 11585 ms'
    """
    with open(filepath, 'r') as f:
        content = f.read()
    
    match = re.search(r'time cost:\s*([\d\.]+)\s*ms', content)
    if match:
        return float(match.group(1))
    else:
        print(f"Warning: Could not find 'time cost' in file {filepath}. Defaulting to 0.0 ms.")
        return 0.0  # Fallback if not found
    
def add_total_execution_time(plan_json, filepath):
    """
    Given a query plan (parsed JSON), this function extracts the total execution
    time from the root node and adds it to the JSON output.
    """
    # Extract total execution time directly from the root node's actual time (operator_time)
    root_node = plan_json['data']
    
    # Directly use the total time of the root node as the total execution time
    # total_execution_time = root_node.get(OP_TIME_END_KEY, 0)

    # For Spark
    total_execution_time = extract_total_execution_time_spark(filepath)
    
    # Add total execution time to the root node's data
    root_node['total_execution_time'] = total_execution_time
    # plan_json["Execution Time"] = total_execution_time
    # print(f"Total execution time for the plan: {total_execution_time}")
    
    return plan_json


def fill_missing_hash_node_values(plan):
    """
    Fill missing values for hash or similar nodes in the query plan by using information
    from parent or child nodes.
    """
    if "Node Type" not in plan:
        return plan  # If the current node does not have a "Node Type", skip it

    # If the node type is "Hash" or any other node that might miss values, attempt to fill them in
    if plan.get("Node Type") in ["Hash", "Aggregate", "Materialize", "Sort"]:
        if plan.get("Actual Startup Time") is None and plan.get("Plans"):
            # Use the startup time from the first child if available
            plan["Actual Startup Time"] = plan["Plans"][0].get("Actual Startup Time") or 0.0

        if plan.get("Actual Total Time") is None and plan.get("Plans"):
            plan["Actual Total Time"] = sum(
                child.get("Actual Total Time") or 0.0 for child in plan["Plans"]
            )

        if plan.get("Actual Rows") is None and plan.get("Plans"):
            plan["Actual Rows"] = max(
                child.get("Actual Rows") or 0.0 for child in plan["Plans"]
            )

        if plan.get("Actual Loops") is None:
            # Default to 1.0 when there is no specified loop count
            plan["Actual Loops"] = 1.0

    # Recursively process all child nodes
    if "Plans" in plan:
        for child in plan["Plans"]:
            fill_missing_hash_node_values(child)

    return plan

import json
def transform_to_postgres_style(json_data):
    def transform_node(node):
        # Map the data keys to PostgreSQL-style keys
        transformed = {
            "Node Type": node["data"]["operator"],
            "Actual Startup Time": node["data"].get("actual_time_start"),
            "Actual Total Time": node["data"].get("actual_time_end"),
            "Actual Rows": node["data"].get("actual_rows"),
            "Actual Loops": node["data"].get("actual_loops")
        }
        
        # Handle auxilary_info -> Filter and table_name -> Relation Name

        aux_info = node["data"].get("auxilary_info")
        if aux_info not in [None, 'null']:
            if node["data"]["operator"] == 'Filter':
                transformed["Filter"] = aux_info
                # print(f"Operator: {node["data"]["operator"]}, added filter:{aux_info}")
            
        if node["data"].get("table_name"):
            transformed["Relation Name"] = node["data"]["table_name"]
        
        # Handle cost and rows
        if node["data"].get("operator_estimate_cost"):
            transformed["Total Cost"] = node["data"]["operator_estimate_cost"]
        if node["data"].get("input_rows"):
            transformed["Plan Rows"] = node["data"]["input_rows"]
        if node["data"].get("plan_rows") is not None:
            transformed["Plan Rows"] = node["data"]["plan_rows"]
        # print(f" [06/11] Updated Plan Rows for root node: {plan['Plan'].get('Plan Rows', 'N/A')}")


         # Change Filter to Sort Key if Node Type is "Sort"
        if transformed["Node Type"] == "Sort" and "Filter" in transformed:
            transformed["Sort Key"] = transformed.pop("Filter")

        # Recursively handle children
        if node.get("children"):
            transformed["Plans"] = [transform_node(child) for child in node["children"]]
        
        return transformed
    
    # Transform the root node
    transformed_plan = transform_node(json_data)
    
    transformed_plan = fill_missing_hash_node_values(transformed_plan)

    # Return the structure with "Execution Time" as a separate key
    return {
        "Plan": transformed_plan,
        "Execution Time": json_data["data"]["total_execution_time"]
    }


import sys
import json
import sys
import json

def merge_filter_and_scan_nodes_recursive(plan):
    if isinstance(plan, dict) and plan.get("Node Type") == "Filter":
        filter_condition = plan.get("Filter", "")
        if not filter_condition:
            return plan  # Nothing to merge

        # Remove the filter node that matches the specific condition (not in postgres)
        if filter_condition == " (supplier.s_suppkey is not null)":
            if plan.get("Plans"):
                return merge_filter_and_scan_nodes_recursive(plan["Plans"][0])  # Continue with the first child node

    # Check if the current plan is a Filter node with a child
    if isinstance(plan, dict) and plan.get("Node Type") == "Filter" and plan.get("Plans"):
        next_node = plan["Plans"][0]  # Assume the first child node is the next in line

        # Check if the next node is a scan-type node
        if next_node.get("Node Type") in [
            "Table scan", "Index lookup", "Single-row index lookup", "Seq Scan",
            "Covering index lookup", "Index range scan", "Inner hash join"
        ]:
            merged_node = {
                "Node Type": next_node.get("Node Type"),
                "Actual Startup Time": next_node.get("Actual Startup Time", 0.0),
                "Actual Total Time": plan.get("Actual Total Time", 0.0),
                "Actual Rows": plan.get("Actual Rows", 0.0),
                "Plan Rows": next_node.get("Plan Rows", 1.0),
                "Relation Name": next_node.get("Relation Name"),
                "Total Cost": next_node.get("Total Cost", 0.0),
                "Filter": plan.get("Filter", ""),  
                "Actual Loops": plan.get("Actual Loops", 1.0)
            }

            # Copy additional fields from the scan node that should be preserved
            for key in next_node:
                if key not in merged_node:
                    merged_node[key] = next_node[key]

            # Recursively merge the children of the scan node
            if next_node.get("Plans"):
                merged_node["Plans"] = [merge_filter_and_scan_nodes_recursive(child) for child in next_node["Plans"]]

            return merged_node

        # Handle Filter + Join node merging
        elif next_node.get("Node Type") in [
            "Nested loop left join", "Nested loop inner join", "Hash join", "Merge join"
        ]:
            next_filter = next_node.get("Filter", "")
            plan_filter = plan.get("Filter", "")
            if next_filter and plan_filter:
                next_node["Filter"] = f"({next_filter}) AND ({plan_filter})"
            else:
                next_node["Filter"] = next_filter or plan_filter

            # Recursively process children
            next_node["Plans"] = [merge_filter_and_scan_nodes_recursive(child) for child in next_node.get("Plans", [])]

            return next_node

        else:
            print(f"Warning: Unexpected node following Filter: {next_node.get('Node Type')}. Proceeding with caution.")
            return plan  # Keep the filter as-is

    # If the current node is not a "Filter", process its children recursively
    if isinstance(plan, dict) and plan.get("Plans"):
        plan["Plans"] = [merge_filter_and_scan_nodes_recursive(child) for child in plan["Plans"]]

    return plan


# Does not add to any information, and affects filter parsing
def remove_specific_filter_nodes(plan):
    if isinstance(plan, dict) and plan.get("Node Type") == "Filter":
        # Check if the filter condition matches the one we want to remove
        if plan.get("Filter") == " (supplier.s_suppkey is not null)":
            # If this node has children, return the first child to effectively remove the current filter node
            return plan.get("Plans", [None])[0]

    # Recursively process child nodes
    if isinstance(plan, dict) and plan.get("Plans"):
        plan["Plans"] = [remove_specific_filter_nodes(child) for child in plan["Plans"] if child]
    
    return plan

def extract_alias_from_filter(filter_condition):
    """
    Extracts table aliases from the filter condition using a regex pattern.
    Handles nested expressions and subqueries.
    """
    # Regex pattern to capture table aliases like 'partsupp.', 'n2.', etc.
    pattern = re.compile(r'\b([a-zA-Z0-9_]+)\.\b[a-zA-Z0-9_]+\b')
    matches = pattern.findall(filter_condition)
    return matches

def infer_table_name_from_expression(expression):
    """
    Infers potential table names from a SQL expression that could involve nested subqueries.
    """
    aliases = extract_alias_from_filter(expression)
    inferred_relations = set(aliases)

    # inferred_relations = {alias_to_table.get(alias) for alias in aliases if alias in alias_to_table}
    return inferred_relations

def fill_missing_parts(plan):
    """
    Fills missing relation names, est_cost, est_rows in the query plan by inferring from filter conditions or node details.
    """
    if "Relation Name" not in plan or plan.get("Relation Name") is None:
        if "Filter" in plan:
            # Extract potential relation names from the filter condition
            inferred_relations = infer_table_name_from_expression(plan["Filter"])
            
            # If a unique relation name is inferred, assign it
            if len(inferred_relations) == 1:
                plan["Relation Name"] = inferred_relations.pop()
            # else:
            #     print(f"Ambiguous or no relations inferred from expression: {plan['Filter']}")

    # Fill in missing Plan rows
    if "Plan Rows" not in plan or plan.get("Plan Rows") is None:
        # If child nodes exist, use the sum of child Plan Rows or if not available, 0 for that node to estimate Plan Rows
        if "Plans" in plan and plan["Plans"]:
            plan_rows_sum = 0  
            for child in plan["Plans"]:
                child_plan_rows = child.get("Plan Rows")
                
                if child_plan_rows is not None:
                    plan_rows_sum += child_plan_rows
                    
            plan["Plan Rows"] = plan_rows_sum if plan_rows_sum > 0 else 1.0
        else:
            plan["Plan Rows"] = 1.0

    # Fill in missing Estimated Total Cost
    if "Total Cost" not in plan or plan.get("Total Cost") is None:
        if "Plans" in plan and plan["Plans"]:
            estimated_cost = 0.0  
            for child in plan["Plans"]:
                child_cost = child.get("Total Cost")
                if child_cost is not None:
                    estimated_cost += child_cost
                else:
                    estimated_cost += 0.0
            if estimated_cost > 0:
                plan["Total Cost"] = estimated_cost
            else:
                plan["Total Cost"] = 0.0
        else:
            plan["Total Cost"] = 0.0

    # Recursively process all child nodes
    if "Plans" in plan:
        for child in plan["Plans"]:
            fill_missing_parts(child)
    
    return plan

# e.g., template 18, mysql outputs: Filter: <in_optimizer>(orders.o_orderkey,orders.o_orderkey in (select #2)) 
def remove_angle_bracket_content(plan):
    """
    Removes any content enclosed in angle brackets (`<>`) from the 'Filter' field in the query plan.
    """
    if "Filter" in plan:
        # Use regex to remove anything between < and >
        # print(f"Before: {plan["Filter"]}")
        plan["Filter"] = re.sub(r'<.*?>', '', plan["Filter"]).strip()
        # print(f"After: {plan["Filter"]}")

    # Recursively process all child nodes
    if "Plans" in plan:
        for child in plan["Plans"]:
            remove_angle_bracket_content(child)

    return plan

# #^ payload
def spark_raw_txt_to_json(infilepath):
    # Initialize Spark parser
    parser = SparkExplainParser()

    # Load raw plan text
    parser.load_raw_plans(infilepath)

    # Parse the text into trees
    parser.parse_raw_plans()

    # Save each parsed plan after calculating total execution time
    all_queries_execution_time = []

    for root in parser.parsed_plan_list:
        root_with_time = add_total_execution_time(root.to_json(), infilepath)
        postgres_style_plan = transform_to_postgres_style(root_with_time)
        cleaned_plan = remove_specific_filter_nodes(postgres_style_plan['Plan'])
        merged_plan = merge_filter_and_scan_nodes_recursive(cleaned_plan)
        filled_plan = fill_missing_parts(merged_plan)
        cleaned_filled_plan = remove_angle_bracket_content(filled_plan)

        all_queries_execution_time.append({
            "Plan": cleaned_filled_plan,
            "Execution Time": postgres_style_plan["Execution Time"]
        })

    return all_queries_execution_time
     
def replace_null_with_zero(obj):
    """
    Recursively replace all None values with 0.0 in a nested dict or list.
    """
    if isinstance(obj, dict):
        return {k: replace_null_with_zero(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_null_with_zero(elem) for elem in obj]
    elif obj is None:
        return 0.0
    else:
        return obj


import os
from tqdm import tqdm
import csv
from collections import defaultdict


import argparse
def process_files(input_folder, output_csv_path, template_range=(1, 101), query_range=(1, 11)):
    output_dir = os.path.dirname(output_csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    id_counter = 1
    missing_templates = []
    missing_queries = defaultdict(list)
    templates_with_errors = set()

    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['id', 'json'])  # Write header

        for template in tqdm(range(*template_range)):
            template_files = [f for f in os.listdir(input_folder) if f.startswith(f"{template}_")]

            if not template_files:
                missing_templates.append(template)
                continue

            for query in range(*query_range):
                filename = f"{template}_{query}_run0.txt"
                txt_src_path = os.path.join(input_folder, filename)

                if not os.path.exists(txt_src_path):
                    missing_queries[template].append(query)
                    continue

                try:
                    result = spark_raw_txt_to_json(txt_src_path)
                    for plan in result:
                        cleaned_plan = replace_null_with_zero(plan)
                        writer.writerow([id_counter, json.dumps(cleaned_plan)])
                        id_counter += 1
                except Exception as e:
                    templates_with_errors.add(template)
                    print(f"Error processing {filename}: {e}")
                    traceback.print_exc()

    # Reporting
    print("\n\nSummary Report:")
    if missing_templates:
        print(f"\nMissing Templates: {missing_templates}")
    if missing_queries:
        print("\nMissing Queries per Template:")
        for template, queries in missing_queries.items():
            print(f"  Template {template}: Missing Queries: {queries}")
    if templates_with_errors:
        print(f"\nTemplates with Errors: {sorted(templates_with_errors)}")
    else:
        print("\nNo templates had processing errors.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert Spark TPC-DS .txt explain outputs to CSV with JSON plans")
    parser.add_argument('--input_folder', type=str, default='.', help='Directory with .txt explain output files')
    parser.add_argument('--output_csv_path', type=str, default='./output.csv', help='Path to output CSV file')
    parser.add_argument('--template_start', type=int, default=1, help='Start of template range (inclusive)')
    parser.add_argument('--template_end', type=int, default=101, help='End of template range (exclusive)')
    parser.add_argument('--query_start', type=int, default=1, help='Start of query range (inclusive)')
    parser.add_argument('--query_end', type=int, default=11, help='End of query range (exclusive)')

    args = parser.parse_args()

    process_files(
        input_folder=args.input_folder,
        output_csv_path=args.output_csv_path,
        template_range=(args.template_start, args.template_end),
        query_range=(args.query_start, args.query_end)
    )
