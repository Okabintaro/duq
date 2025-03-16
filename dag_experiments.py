# %%
from pathlib import Path

from duq.sql_model import SqlModel

all_sql_files = list(Path("examples/jaffle_shop").rglob("*.sql"))

# %%
for f in all_sql_files:
    print(f)

# %%
sql_models = [SqlModel.from_file(f) for f in all_sql_files]

# %%
sql_models

# %%
for m in sql_models:
    print(m.filepath, m.dependencies)

# %%
source_models = [m for m in sql_models if len(m.dependencies) == 0]
print(source_models)

# %%
f = SqlModel.from_file("examples/jaffle_shop/sources/raw_customers.sql")
