pants_requirements(name="pants")

python_requirements(
    name="additional_reqs",
    source="additional_requirements.txt",
)

python_requirements(
    name="tool_reqs",
    source="tool_requirements.txt",
    resolve="tools",
)

__defaults__(
    {
        (python_test, python_tests, python_test_utils): {
            "dependencies": ["//:additional_reqs#pytest-raises"]
        }
    }
)
