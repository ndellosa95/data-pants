from pants.engine.target import SingleSourceField


class DbtSourceField(SingleSourceField):
	"""Source field base class required for dependency inference."""


class DbtSqlSourceField(DbtSourceField):
	"""Base class for dbt sources that correspond to generated SQL."""

	expected_file_extensions = (".sql",)
