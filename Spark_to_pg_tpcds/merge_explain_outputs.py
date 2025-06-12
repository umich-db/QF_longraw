from node import Node
from json import dumps

def parse_number(s):
    suffixes = {'K': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12}
    for suffix, multiplier in suffixes.items():
        if s.endswith(suffix):
            return float(s[:-1]) * multiplier
    return float(s)

def merge_explain_outputs(explain_output, explain_analyze_output: Node):
    if not explain_output:
        return explain_analyze_output
    else:
        try:
            recurse(explain_output['query_block'], explain_analyze_output)
            explain_analyze_output.extracted_data['query_cost'] = parse_number(explain_output['query_block']['cost_info']['query_cost'])
            return explain_analyze_output
        except Exception as e:
            print('Exception when trying to merge: ', dumps(explain_output, indent=2))
            print('Exception', repr(e))

def summary(d):
    return {k: d[k] if type(d[k]) in [int, float, str, bool] else '...' for k in d}

def recurse(e, ea: Node):
    op = ea.extracted_data['operator']
    def recurse_all(e, ea):
        for child in ea.children:
            recurse(e, child)
    try:
        if op == 'Sort':
            op_dict = e['ordering_operation'] if 'ordering_operation' in e else e
            recurse_all(op_dict, ea)
        elif op == 'Aggregate' or op == 'Group aggregate':
            op_dict = e['grouping_operation'] if 'grouping_operation' in e else e
            recurse_all(op_dict, ea)
        elif op == 'Materialize':
            op_dict = e['materialized_from_subquery']['query_block']
            recurse_all(op_dict, ea)
        elif 'join' in op:
            # Join the last item in "nested_loop" with the first child of the join in text data
            op_dict = {} # TODO
            next_index = 0
            def recurse_joins(node):
                node_op = node.extracted_data['operator']
                nonlocal next_index
                if 'join' in node_op:
                    for child_ea in node.children:
                        recurse_joins(child_ea)
                elif node_op == 'Filter':
                    recurse_joins(node.children[0])
                    child_e = e['nested_loop'][next_index - 1]
                    if 'table' in child_e and 'attached_subqueries' in child_e['table']:
                        assert len(node.children) - 1 == len(child_e['table']['attached_subqueries'])
                        for i in range(len(node.children) - 1):
                            e_inner = child_e['table']['attached_subqueries'][i]
                            if 'query_block' in e_inner:
                                e_inner = e_inner['query_block']
                            recurse(e_inner, node.children[i + 1])
                else:
                    recurse(e['nested_loop'][next_index], node)
                    next_index += 1
            recurse_joins(ea)
        elif op == 'Filter':
            op_dict = {}
            recurse(e, ea.children[0])
            # For now assume that filter subqueries only occur on "table" keys
            if 'table' in e and 'attached_subqueries' in e['table']:
                assert len(ea.children) - 1 == len(e['table']['attached_subqueries'])
                for i in range(len(ea.children) - 1):
                    recurse(e['table']['attached_subqueries'][i]['query_block'], ea.children[i + 1])
        elif op.startswith('Select #') or op == 'Limit' or op == 'Stream results' or op == 'Hash':
            op_dict = {}
            recurse_all(e, ea)
        elif 'index lookup' in op.lower():
            op_dict = e['table']
            recurse_all(op_dict, ea)
        elif 'range scan' in op or 'index scan' in op.lower():
            op_dict = e['table']
            recurse_all(op_dict, ea)
        elif op == 'Table scan':
            on_temporary = '<temporary>' in ea.raw_data
            op_dict = {} if on_temporary else e['table']
            # TODO table scan on temporary = no op
            recurse_all(e if on_temporary else e['table'], ea)
        else:
            raise ValueError(f'Unknown operation: {op}')
        # Extract useful data
        if 'cost_info' in op_dict:
            for key, value in op_dict['cost_info'].items():
                ea.extracted_data[key] = parse_number(value)
            for key in ['key_length', 'filtered']:
                if key in op_dict:
                    ea.extracted_data[key] = parse_number(op_dict[key])
            for key in ['access_type', 'using_filesort', 'using_temporary_table', 'rows_examined_per_scan', 'rows_examined_per_join']:
                if key in op_dict:
                    ea.extracted_data[key] = op_dict[key]

    except Exception as ex:
        print(op, summary(e))
        raise ex