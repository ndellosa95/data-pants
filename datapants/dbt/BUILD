python_sources()

files(
    name="sample-project", 
    sources=["sample-project/**", "test_lockfiles/*.txt"],
)

relocated_files(
    name="sample-project-relocated",
    files_targets=[":sample-project"],
    src=str(build_file_dir()),
    dest="",
)

python_tests(
    name="tests",
    dependencies=[":sample-project-relocated", "//:additional_reqs#pytest-raises"],
)
