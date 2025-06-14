from enum import Enum

# constants
OP_NAME_KEY = "operator"
COST_KEY = "estimate_cost_start"
ROWS_KEY = "estimate_rows"
OP_TIME_KEY = "operator_time"
OP_TIME_START_KEY = "operator_time_start"
OP_TIME_END_KEY = "operator_time_end"
OP_COST_KEY = "operator_estimate_cost"
DATA_KEY = 'data'
CHILDREN_KEY = 'children'
ACTUAL_ROWS_KEY = 'actual_rows'
LABEL_KEY = OP_TIME_KEY
OP_ACTUAL_TIME_START_KEY = 'actual_time_start'
OP_ACTUAL_TIME_END_KEY = 'actual_time_end'
ACTUAL_LOOPS_KEY = "actual_loops"
INPUT_ROWS_KEY = 'input_rows'
TABLE_NAME_KEY = 'table_name'
# extended features
QUERY_COST_KEY = 'ex_query_cost'
READ_COST_KEY = 'ex_read_cost'
PREFIX_COST_KEY = 'ex_prefix_cost'
EVAL_COST_KEY = 'ex_eval_cost'
SORT_COST_KEY = 'ex_sort_cost'
KEY_LENGTH_KEY = 'ex_key_length'
FILTERED_KEY = 'ex_filtered'
ACCESS_TYPE_KEY = 'ex_access_type'
USING_FILESORT_KEY = 'ex_using_filesort' # PG equivalent: Sort Space Type == Disk
USING_TEMPORARY_TABLE_KEY = 'ex_using_temporary_table'
ROWS_EXAMINED_PER_SCAN_KEY = 'ex_rows_examined_per_scan'
ROWS_EXAMINED_PER_JOIN_KEY = 'ex_rows_examined_per_join'

STANDARD_FEATURES = [COST_KEY, ROWS_KEY, INPUT_ROWS_KEY, OP_NAME_KEY]
ENSEMBLE_FEATURES = [OP_TIME_KEY] + STANDARD_FEATURES
CARD_FEATURES = [ACTUAL_ROWS_KEY] + STANDARD_FEATURES
EXTENDED_FEATURES = STANDARD_FEATURES + [QUERY_COST_KEY, READ_COST_KEY, PREFIX_COST_KEY, EVAL_COST_KEY, SORT_COST_KEY, KEY_LENGTH_KEY, FILTERED_KEY, ACCESS_TYPE_KEY, USING_FILESORT_KEY, USING_TEMPORARY_TABLE_KEY, ROWS_EXAMINED_PER_SCAN_KEY, ROWS_EXAMINED_PER_JOIN_KEY]
ENSEMBLE_EXTENDED_FEATURES = [OP_TIME_KEY] + EXTENDED_FEATURES
CARD_EXTENDED_FEATURES = [ACTUAL_ROWS_KEY] + EXTENDED_FEATURES

numerical_keys = {
    COST_KEY,
    ROWS_KEY,
    OP_ACTUAL_TIME_START_KEY,
    OP_ACTUAL_TIME_END_KEY,
    ACTUAL_ROWS_KEY,
    ACTUAL_LOOPS_KEY,
}

## LABEL TYPES
class Labels(Enum):
    Time = 1
    Card = 2

# training mode
ENSEMBLE_MODEL_TYPE = 'ensemble'
EXTENDED_MODEL_TYPE = 'extended'
CARD_MODEL_TYPE = 'cardinality'
CARD_EXTENDED_MODEL_TYPE = 'cardinality_extended'

# testing stage
OP_PRED_TIME_KEY = 'predicted_operator_time'
OP_PRED_ACC_TIME_KEY = 'predicted_accumulated_time'
OP_ACTUAL_ACC_TIME_KEY = 'actual_accumulated_time'

# operator type 
TRAINABLE_INFO = {
    "trainable": [
        "Covering index lookup",
        "Covering index scan",
        "Filter",
        "Group aggregate",
        "Index lookup",
        "Index range scan",
        "Index scan",
        "Inner hash join",
        "Limit",
        "Materialize",
        "Materialize with deduplication",
        "Nested loop antijoin",
        "Nested loop inner join",
        "Nested loop left join",
        "Nested loop semijoin",
        "Single-row index lookup",
        "Stream results",
        "Table scan"
    ],
    "untrainable_executed": [
        "Aggregate",
        "Sort"
    ],
    "untrainable_not_executed": [
        "Hash",
        "Select"
    ]
}


# operator mapping
manual_operator_mapping = {
    "MS_TO_PG": { # key: ms operator ; value: pg operator
        "Sort": "Sort",
        "Covering index scan": "Index Only Scan",
        "Index range scan": "Index Scan",
        "Index scan": "Index Scan",
        "Table scan": "Seq Scan",
        "Inner hash join": "Hash Join",
        "Nested loop antijoin": "Merge Join",
        "Nested loop inner join": "Merge Join",
        "Nested loop left join": "Merge Join",
        "Nested loop semijoin": "Merge Join",
        "Aggregate": "Aggregate",
        "Group aggregate": "Aggregate", # FIXME:
        "Covering index lookup": "Index Only Scan",
        "Filter": "Gather", # FIXME:
        "Hash": "Hash",
        "Index lookup": "Index Scan",
        "Limit": "Materialize", # FIXME:
        "Materialize": "Materialize",
        "Select": "Seq Scan", # FIXME:
        "Single-row index lookup": "Index Scan",
        "Stream results": "Gather",
    }
}

old_data_key_map = {
    "Node Type": OP_NAME_KEY,
    "Total Cost": COST_KEY,
    "Plan Rows": ROWS_KEY,
    "Actual Startup Time": OP_ACTUAL_TIME_START_KEY,
    "Actual Total Time": OP_ACTUAL_TIME_END_KEY,
    "Actual Rows": ACTUAL_ROWS_KEY,
    "Actual Loops": ACTUAL_LOOPS_KEY,
    "Plans": CHILDREN_KEY,
}