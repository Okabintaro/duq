# %%
read_csv_string = """
SELECT *
FROM read_csv('examples/jaffle_shop/sources/raw_customers.csv')
"""
import sqlglot

tree = sqlglot.parse_one(read_csv_string, dialect="duckdb")
print(tree)

# %%
tree

# %%
# Identify the source/raw tables

