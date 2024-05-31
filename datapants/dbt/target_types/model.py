from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, Target

from .base import DbtSourceField


class DbtModelSourceField(DbtSourceField):
	help = "The source file for this dbt model."


class DbtModel(Target):
	alias = "dbt_model"
	help = "A target representing a single dbt model."
	core_fields = (*COMMON_TARGET_FIELDS, DbtModelSourceField, Dependencies)
