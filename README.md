# QF_longraw
## Currently written for MySQL, DuckDB, Spark (both TPC-H and TPC-DS)
Code for generating long_raw.csv for QueryFormer (to match the PostgreSQL EXPLAIN ANALYZE output format)

To run the main scripts: 

`python spark_to_pg.py --input_folder <path> --output_csv_path <path> --template_start <s> --template_end <e> --query_start <s> --query_end <e>`

`python mysql_to_pg.py --input_folder <path> --output_csv_path <path> --template_start <s> --template_end <e> --query_start <s> --query_end <e> --op_categories <path> `

`python duckdb_to_pg.py --input_folder <path> --output_csv_path <path> --template_start <s> --template_end <e> --query_start <s> --query_end <e> --map_operator_type <True or False>`
