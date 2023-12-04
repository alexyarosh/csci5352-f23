# Solution to Lab 02

- `analysis` folder contains the SQL queries for the analysis part
- `data models` folder contains descriptions of 3NF and dimensional data models
- `db` contains database initialization script
- `etl_scripts` contains the Python scripts for extracting, loading, and transforming data
- `notebooks` contains expolatory notebooks that I used to figure out the code



### Note:

Once we introduce relationships between tables, `df.to_sql()` generally becomes difficult to work with, because we need to explicitly provide keys for all the fields in the relationships. For example, we need to manually create the keys for the date dimension, and use those keys explicitly in the fact table.

A better way is to either just use SQL to transform data (either directly, or through, for example, duckdb Python package), or use the ORM functionality of `sqlalchemy`.

However, for this small project, we'll specify all the keys manually in the Python code.