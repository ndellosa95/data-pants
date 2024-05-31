from pants.engine.target import COMMON_TARGET_FIELDS, Dependencies, Target

from .base import DbtSourceField

__test__ = False


class DbtTestSourceField(DbtSourceField):
	help = "The source file for this dbt test."


class DbtTest(Target):
	alias = "dbt_test"
	help = "A target representing a single dbt test."
	core_fields = (*COMMON_TARGET_FIELDS, DbtTestSourceField, Dependencies)
