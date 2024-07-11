from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, Target

from .base import DbtSqlSourceField


class DbtMacroSourceField(DbtSqlSourceField):
	help = "The source file for this dbt macro."


class DbtMacro(Target):
	alias = "dbt_macro"
	help = "A target representing a single dbt macro."
	core_fields = (*COMMON_TARGET_FIELDS, DbtMacroSourceField, Dependencies)
