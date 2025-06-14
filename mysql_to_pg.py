'''
Convert MySQL EXPLAIN ANALYZE json&txt files (but only extracting txt parts) to a single csv file with [id, json] 
'''

# Define the input folder (where .txt files are located) and the output folder (for .json files)
# input_folder = 'C:/Research/2023_Research/Foundation_Database/2025_05_code/Mysql_tpcds'
# output_csv_path = 'C:/Research/2023_Research/Foundation_Database/2025_05_code/QueryFormer/data/tpcds_2505/mysql_10g/long_raw.csv'
# op_categories_path = 'C:/Research/2023_Research/Foundation_Database/2025_05_code/Mysql_tpcds/operator_categories.json'

import os
import sys
import json
from collections import deque
from copy import deepcopy
import re
import shutil

from parsers.ms_node import Node, merge_explain_outputs

# sys.path.append(os.path.expanduser('~/Oct/src'))
import parsers.utils_ms as utils_ms

# some constant key names
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

row_cnt_dict = {
    "orders": 15000000,
    "lineitem": 59986052,
    "region": 5,
    "nation": 25,
    "supplier": 100000,
    "part": 2000000,
    "partsupp": 8000000,
    "customer": 1500000
}

alias_to_table = {
    'n1': 'nation',
    'n2': 'nation',
    'o': 'orders',
    'c': 'customer',
    'ps': 'partsupp',
    's': 'supplier',
}

table_list = ['nation','orders','region','supplier','part','partsupp','lineitem', 'customer']

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

def update_operator_runtime(plan_node, op_categories):
    #^ post-order traversal 
    # process the children
    for subplan in plan_node.children:
        update_operator_runtime(plan_node=subplan, op_categories=op_categories)

    # process the root node
    # calculate startup times and total time
    op_name = reduce_opname(plan_node.extracted_data[OPERATOR_KEY])
    table = extract_target_table(plan_node.extracted_data[OPERATOR_KEY])
    # print(f"op_name: {op_name}")
    # print(f"table: {table}")


    # for not-executed nodes
    if op_name in op_categories[NOT_EXE_KEY] or "never executed" in plan_node.extracted_data[OPERATOR_KEY]:
        # runtime and start_time set to 0.0
        plan_node.extracted_data[OP_TIME_KEY] = 0.0
        plan_node.extracted_data[OP_TIME_START_KEY] = 0.0
        # total time is the sum of all child subplans' total runtime (namely, assume no parallelism)
        subplan_total_time = sum([subplan.extracted_data[OP_TIME_END_KEY] for subplan in plan_node.children]) if (len(plan_node.children) > 0) else 0.0
        plan_node.extracted_data[OP_TIME_END_KEY] = subplan_total_time
        return

    # for executed nodes
    try:
        actual_time_start = float(plan_node.extracted_data.get(ACTUAL_TIME_START_KEY, 0.0))
        actual_time_end = float(plan_node.extracted_data.get(ACTUAL_TIME_END_KEY, 0.0))
        loops = float(plan_node.extracted_data.get(ACTUAL_LOOPS_KEY, 1.0))
        
        # print(f"Actual start time: {actual_time_start}, Actual end time: {actual_time_end}, Loops: {loops}")

        # Calculate operator time considering loops
        plan_node.extracted_data[OP_TIME_START_KEY] = actual_time_start * loops
        plan_node.extracted_data[OP_TIME_END_KEY] = actual_time_end * loops

        # print(f"Calculated OP_TIME_START: {plan_node.extracted_data[OP_TIME_START_KEY]}")
        # print(f"Calculated OP_TIME_END: {plan_node.extracted_data[OP_TIME_END_KEY]}")

        # Sum of subplan (child) execution times
        subplan_total_time = sum([subplan.extracted_data[OP_TIME_END_KEY] for subplan in plan_node.children]) if plan_node.children else 0.0

        # Calculate operator's execution time (subtract child execution time from total)
        plan_node.extracted_data[OP_TIME_KEY] = plan_node.extracted_data[OP_TIME_END_KEY] - subplan_total_time

        # print(f"Subplan total time: {subplan_total_time}")
        # print(f"Operator time for {op_name} (excluding children): {plan_node.extracted_data[OP_TIME_KEY]}")
        # print(f"Total time for {op_name} (including children): {plan_node.extracted_data[OP_TIME_END_KEY]}\n")

        # Update operator name and table name
        plan_node.extracted_data[OPERATOR_KEY] = op_name
        if table is not None:
            plan_node.extracted_data[utils_ms.TABLE_NAME_KEY] = table

        # plan_node.extracted_data[OP_TIME_START_KEY] = float(plan_node.extracted_data.get(ACTUAL_TIME_START_KEY, 0)) * float(plan_node.extracted_data.get(ACTUAL_LOOPS_KEY, 1))
        # plan_node.extracted_data[OP_TIME_END_KEY] = float(plan_node.extracted_data.get(ACTUAL_TIME_END_KEY, 0)) * float(plan_node.extracted_data.get(ACTUAL_LOOPS_KEY, 1))
        # subplan_total_time = sum([subplan.extracted_data[OP_TIME_END_KEY] for subplan in plan_node.children]) if (len(plan_node.children) > 0) else 0.0
        # plan_node.extracted_data[OP_TIME_KEY] = plan_node.extracted_data[OP_TIME_END_KEY] - subplan_total_time
        # plan_node.extracted_data[OPERATOR_KEY] = op_name
        # if table is not None:
        #     plan_node.extracted_data[utils_ms.TABLE_NAME_KEY] = table
    except KeyError as e:
        raise KeyError(f"error key: {e}, plan:\n{plan_node.data}") from e

def update_input_rows(plan_node):
    for subplan in plan_node.children:
        update_input_rows(plan_node=subplan)
    # 1. for leaf node, load the table row count from prepared external file
    # 2. for non-leaf node, pick the maximum estimate row count among children
    if len(plan_node.children) == 0: # is leaf node
        try:
            plan_node.extracted_data[utils_ms.INPUT_ROWS_KEY] = row_cnt_dict.get(plan_node.extracted_data.get(utils_ms.TABLE_NAME_KEY), 0.0)
        except KeyError:
            plan_node.extracted_data[utils_ms.INPUT_ROWS_KEY] = float(plan_node.extracted_data.get(ROWS_KEY, 0.0)) if plan_node.extracted_data.get(ROWS_KEY) is not None else 0.0
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


class MysqlPlanParser():
    def __init__(self, op_categories):
        self.op_categories_ = op_categories

    def parse_one_plan(self, plan_chuck: str):
        def merge_stack(stack, stop_indent_level):
            '''merge an incremental stack of (node, indent) pair
            '''
            children_buffer = [] # storing children for parents
            # print(f"Starting merge_stack with stop_indent_level={stop_indent_level}")
            # print(f"Initial stack (before merging):")
            # for idx, (node, indent) in enumerate(stack):
            #     print(f"  Stack[{idx}] -> Node: {node.extracted_data['operator']} (Indent Level: {indent})")
        
            while (len(stack) >= 2 and stack[-1][1] > stop_indent_level): # stop when stack is small or already merged at required indent level
                top = stack[-1]; stack.pop()
                children_buffer.insert(0, top[0])
                # print(f"  Merging: {top[0].extracted_data['operator']} (Indent Level: {top[1]})")

                if (stack[-1][1] > top[1]):
                    raise ValueError("children node cannot have smaller indentation!")
                elif (stack[-1][1] == top[1]): # siblings
                    continue
                else: # parent
                    stack[-1][0].children = deepcopy(children_buffer)
                    children_buffer.clear()
            # print(f"Stack after merging:")
            # for idx, (node, indent) in enumerate(stack):
            #     print(f"  Stack[{idx}] -> Node: {node.extracted_data['operator']} (Indent Level: {indent})")
            # print("\n")

        '''parse on plan chunks '''
        lines = plan_chuck.replace('\\n', '\n').splitlines()
        stack = deque()
        for line in lines:
            line = line.rstrip()  # Remove leading and trailing spaces
            if line == "":
                # print("Skipping empty line.")
                continue  # Skip empty lines

            # print(f"Curr line: {line}")
            if ("EXPLAIN" in line):
                continue

            # count the indentation, decide whether it's a sibling or child
            # this_indent_level = len(line) - len(line.lstrip(' \t')) # count of indentations ahead
            
            leading_spaces = len(line) - len(line.lstrip(' '))
            leading_tabs = len(line) - len(line.lstrip('\t'))
            this_indent_level = leading_spaces + (leading_tabs * 4)  # Assume tabs count as 4 spaces
            this_node = Node(line.strip())

            # print("Current Stack:")
            # for idx, (node, indent) in enumerate(stack):
                # print(f"  Stack[{idx}] -> Node: {node.extracted_data['operator']} (Indent Level: {indent})")
                
            if (len(stack) > 0):
                prev_indent_level = stack[-1][1]
                if prev_indent_level > this_indent_level : # stack top is a child of its sibling among the stack
                    # keep merging the stack from top until find the sibling 
                    merge_stack(stack, this_indent_level)
                stack.append((this_node, this_indent_level))

            else: # entry point 
                stack.append((this_node, this_indent_level))

            # Visualize the updated stack after each line is processed
            # print("Updated Stack After Line:")
            # for idx, (node, indent) in enumerate(stack):
            #     print(f"  Stack[{idx}] -> Node: {node.extracted_data['operator']} (Indent Level: {indent})")
            # print("\n")

        # merge the rest of the nodes in stack
        merge_stack(stack, 0)
        # check validity of stack
        if (len(stack) >= 2):
            # print(stack)
            raise ValueError("length of output stack shouldn't be more than 1!")
        elif (len(stack) == 0):
            raise ValueError("Empty stack! Impossible unless the file is empty")
        
        update_looped_values(stack[0][0])
        update_operator_runtime(stack[0][0], self.op_categories_) #^ calculate the each operator runtime and add to the data dict
        update_input_rows(stack[0][0])
        update_operator_cost(stack[0][0])
        return stack[0][0]

class MysqlPlanChuck:
    def __init__(self, raw_string):
        # raw_string should contain EXPLAIN\n{...json}\nEXPLAIN\n->query plan nodes
        # split at second EXPLAIN (stores index of second "EXPLAIN")
        # split = [_ for _ in re.finditer('EXPLAIN', raw_string)][1].start()
        # self.explain_output = raw_string[:split]
        # self.explain_analyze_output = raw_string[split:]
        self.explain_output = None
        self.explain_analyze_output = raw_string

            
class MysqlExplainParser():
    raw_plan_list: list[MysqlPlanChuck]

    def __init__(self, op_categories):
        self.raw_plan_list = []
        self.parsed_plan_list = None # list of root nodes, each representing a query plan
        self.plan_parser = MysqlPlanParser(op_categories=op_categories)
    
    def load_raw_plans(self, filename):
        ''' Extracts the second EXPLAIN block (everything after the second EXPLAIN) '''
        with open(filename, 'r') as infile:
            content = infile.read()
            
            # Find all occurrences of "EXPLAIN" in the file
            explain_matches = [m.start() for m in re.finditer(r'EXPLAIN', content)]
            
            if len(explain_matches) < 2:
                raise ValueError(f"File {filename} does not contain at least two EXPLAIN sections.")
            
            # Extract everything after the second EXPLAIN
            second_explain_pos = explain_matches[1]
            explain_block = content[second_explain_pos:]
            
            # Split the remaining block into separate plans if needed
            out_list = explain_block.split('\n\n')  # This remains unchanged
            self.raw_plan_list = [MysqlPlanChuck(plan) for plan in out_list if not (plan.isspace() or len(plan) == 0)]
        
        # ''' splits original files into chucks, each chuck refers to one plan. Return the list of chunks
        # '''
        # # print(f"Loading file: {filename}")
        # with open(filename, 'r') as infile:
        #     content = infile.read()
        #     out_list = content.split('\n\n')
        #     self.raw_plan_list = [MysqlPlanChuck(plan) for plan in out_list if not (plan.isspace() or len(plan) == 0)]
        
        # # for i, plan_chunk in enumerate(self.raw_plan_list[:5]):  # Limit the output to first 5 chunks for brevity
        # #     print(f"Plan chunk {i+1}:\n{plan_chunk.explain_analyze_output}...\n")  
   

    def parse_explain_output(self, explain_output):
        # Assume first line contains EXPLAIN, rest is valid JSON
        explain_output = explain_output.split('\n', 1)[1]
        return json.loads(explain_output)

    def parse_raw_plans(self):
        if (len(self.raw_plan_list) == 0):
            # print("Plan list is empty! Load the plan from file first!")
            return
        self.parsed_plan_list = []
        for plan in self.raw_plan_list:
            # explain_output = self.parse_explain_output(plan.explain_output)
            explain_output = {}
            plan_node = self.plan_parser.parse_one_plan(plan.explain_analyze_output)
            self.parsed_plan_list.append(merge_explain_outputs(explain_output, plan_node))

    # def print_raw_plans(self):
    #     for i, plan_txt in enumerate(self.raw_plan_list):
    #         print(f"section {i}:")
    #         print(plan_txt)

    # def print_parsed_plans(self):
    #     for i, plan_tree in enumerate(self.parsed_plan_list):
    #         print(f"section {i}:")
    #         print(plan_tree)


def add_total_execution_time(plan_json):
    """
    Given a query plan (parsed JSON), this function extracts the total execution
    time from the root node and adds it to the JSON output.
    """
    # Extract total execution time directly from the root node's actual time (operator_time)
    root_node = plan_json['data']
    
    # Directly use the total time of the root node as the total execution time
    total_execution_time = root_node.get(OP_TIME_END_KEY, 0)

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
            plan["Actual Startup Time"] = plan["Plans"][0].get("Actual Startup Time", 0.0)

        if plan.get("Actual Total Time") is None and plan.get("Plans"):
            plan["Actual Total Time"] = sum(
                child.get("Actual Total Time", 0.0) for child in plan["Plans"]
            )

        if plan.get("Actual Rows") is None and plan.get("Plans"):
            plan["Actual Rows"] = max(
                child.get("Actual Rows", 0.0) for child in plan["Plans"]
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
    inferred_relations = {
        alias if alias in table_list else alias_to_table.get(alias)
        for alias in aliases
    }
    inferred_relations = {relation for relation in inferred_relations if relation is not None}

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

#^ payload     
def mysql_raw_txt_to_json(infilepath, op_categories_path):
    # load operator categories
    with open(op_categories_path, 'r', encoding='utf-8') as infile:
        op_categories = json.load(infile)
    # init the file parser
    parser = MysqlExplainParser(op_categories=op_categories)
    # load raw plan text
    parser.load_raw_plans(infilepath)
    # parse the text into trees
    parser.parse_raw_plans()


    # Save each parsed plan after calculating total execution time
    all_queries_execution_time = []
    
    for root in parser.parsed_plan_list:
        root_with_time = add_total_execution_time(root.to_json())
        postgres_style_plan = transform_to_postgres_style(root_with_time)
        cleaned_plan = remove_specific_filter_nodes(postgres_style_plan['Plan'])
        merged_plan = merge_filter_and_scan_nodes_recursive(cleaned_plan)
        filled_plan = fill_missing_parts(merged_plan)
        cleaned_filled_plan = remove_angle_bracket_content(filled_plan)

        # all_queries_execution_time.append({"Plan": merged_plan})
        all_queries_execution_time.append({
            "Plan": filled_plan,
            "Execution Time": postgres_style_plan["Execution Time"]
        })
    
    return all_queries_execution_time


import os
from tqdm import tqdm
import csv
from collections import defaultdict

import argparse
def process_files(input_folder, output_csv_path, op_categories_path, template_range=(1, 101), query_range=(1, 11)):
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
                print(f"template:{template}, query:{query}")

                filename = f"{template}_{query}_run0.txt"
                txt_src_path = os.path.join(input_folder, filename)

                if not os.path.exists(txt_src_path):
                    missing_queries[template].append(query)
                    continue

                try:
                    result = mysql_raw_txt_to_json(
                        infilepath=txt_src_path,
                        op_categories_path=op_categories_path
                    )

                    for plan in result:
                        writer.writerow([id_counter, json.dumps(plan)])
                        id_counter += 1

                except Exception as e:
                    templates_with_errors.add(template)
                    print(f"Error processing {filename}: {e}")
                    # traceback.print_exc()

    print("\n\nSummary Report:")
    if missing_templates:
        print(f"\nMissing Templates: {missing_templates}")
    else:
        print("\nNo templates are missing.")

    if missing_queries:
        print("\nMissing Queries per Template:")
        for template, queries in missing_queries.items():
            print(f"  Template {template}: Missing Queries: {queries}")
    else:
        print("\nNo individual queries are missing.")

    if templates_with_errors:
        print(f"\nTemplates with Errors: {sorted(templates_with_errors)}")
    else:
        print("\nNo templates had processing errors.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert MySQL TPC-DS .txt explain outputs to CSV with JSON plans")
    parser.add_argument('--input_folder', type=str, required=True, help='Directory with .txt explain output files')
    parser.add_argument('--output_csv_path', type=str, required=True, help='Path to output CSV file')
    parser.add_argument('--op_categories_path', type=str, required=True, help='Path to operator categories JSON file')
    parser.add_argument('--template_start', type=int, default=1, help='Start of template range (inclusive)')
    parser.add_argument('--template_end', type=int, default=101, help='End of template range (exclusive)')
    parser.add_argument('--query_start', type=int, default=1, help='Start of query range (inclusive)')
    parser.add_argument('--query_end', type=int, default=11, help='End of query range (exclusive)')

    args = parser.parse_args()

    process_files(
        input_folder=args.input_folder,
        output_csv_path=args.output_csv_path,
        op_categories_path=args.op_categories_path,
        template_range=(args.template_start, args.template_end),
        query_range=(args.query_start, args.query_end)
    )
