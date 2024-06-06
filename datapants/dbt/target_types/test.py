from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, Target

from .base import DbtSqlSourceField

__test__ = False


class DbtTestSourceField(DbtSqlSourceField):
	help = "The source file for this dbt test."


class DbtTest(Target):
	alias = "dbt_test"
	help = "A target representing a single dbt test."
	core_fields = (*COMMON_TARGET_FIELDS, DbtTestSourceField, Dependencies)
