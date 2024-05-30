from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, SingleSourceField, Target

__test__ = False


class DbtTestSourceField(SingleSourceField):
	expected_file_extensions = (".sql",)
	help = "The source file for this dbt test."


class DbtTest(Target):
	alias = "dbt_test"
	help = "A target representing a single dbt test."
	core_fields = (*COMMON_TARGET_FIELDS, DbtTestSourceField, Dependencies)
