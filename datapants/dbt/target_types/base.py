from pants.engine.target import SingleSourceField


class DbtSourceField(SingleSourceField):
	"""Base class for dbt sources that correspond to generated SQL."""

	expected_file_extensions = (".sql",)
