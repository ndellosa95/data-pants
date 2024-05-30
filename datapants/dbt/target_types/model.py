from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, SingleSourceField, Target


class DbtModelSourceField(SingleSourceField):
	expected_file_extensions = (".sql", ".py")
	help = "The source file for this dbt model."


class DbtModel(Target):
	alias = "dbt_model"
	help = "A target representing a single dbt model."
	core_fields = (*COMMON_TARGET_FIELDS, DbtModelSourceField, Dependencies)
