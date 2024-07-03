test_sources = [
    "test_rules.py",
    "test_third_party_package.py",
]

python_sources(
    sources=["*.py", *(f"!{src}" for src in test_sources)]  # required so it picks up `test.py`
)

python_tests(
    name="tests",
    sources=test_sources,
    dependencies=[f"{build_file_dir().parent}:sample-project-relocated", "//:additional_reqs#pytest-raises"],
    timeout=30,
)
