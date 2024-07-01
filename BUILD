pants_requirements(name="pants")

python_requirements(
    name="additional_reqs",
    source="additional_requirements.txt",
)

__defaults__(
    {
        (python_test, python_tests, python_test_utils): {
            "dependencies": ["//:additional_reqs#pytest-raises"]
        }
    }
)
