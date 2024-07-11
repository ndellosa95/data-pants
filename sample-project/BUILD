python_requirements(
    name="reqs",
)

file(name="db", source="datapants.duckdb")

dbt_project(
    name="root",
    required_adapters=["dbt-duckdb"],
    dependencies=[":db"],
)
