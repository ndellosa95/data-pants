from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, Target

from .base import DbtSourceField


class DbtConfigSourceField(DbtSourceField):
	expected_file_extensions = (".yml", ".yaml")
	help = "The source field for this dbt configuration file."


class DbtConfig(Target):
	alias = "dbt_config"
	help = "A target representing a single dbt configuration file."
	core_fields = (*COMMON_TARGET_FIELDS, DbtConfigSourceField, Dependencies)
	expected_file_extensions = DbtConfigSourceField.expected_file_extensions
