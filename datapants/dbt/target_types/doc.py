from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, SingleSourceField, Target


class DbtDocSourceField(SingleSourceField):
	expected_file_extensions = (".md",)
	help = "The source field for this dbt markdown documentation file."


class DbtDoc(Target):
	alias = "dbt_doc"
	help = "A target representing a single dbt markdown documentation file."
	core_fields = (*COMMON_TARGET_FIELDS, DbtDocSourceField, Dependencies)
	expected_file_extensions = DbtDocSourceField.expected_file_extensions
