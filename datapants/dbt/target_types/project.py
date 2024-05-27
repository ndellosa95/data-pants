from pants.backend.python.target_types import PythonResolveField
from pants.core.util_rules.environments import EnvironmentField
from pants.engine.target import (
	COMMON_TARGET_FIELDS,
	Dependencies,
	OverridesField,
	SingleSourceField,
	StringSequenceField,
	Target,
	TargetGenerator,
)
from pants.util.strutil import softwrap


class ProfilesDirectoryField(SingleSourceField):
	alias = "profiles_dir"
	help = "The path to the directory where the `profiles.yml` for this project is located."
	default = "."


class RequiredAdaptersField(StringSequenceField):
	alias = "required_adapters"
	help = "All possible adapters required by this dbt project."


class DbtProjectTargetGenerator(TargetGenerator):
	alias = "dbt_project"
	help = softwrap(
		"""A target generator representing a single dbt project which generates targets for all of the projects underlying components."""
	)

	generated_target_cls = Target
	core_fields = (
		*COMMON_TARGET_FIELDS,
		Dependencies,
		OverridesField,
		EnvironmentField,
		PythonResolveField,
		RequiredAdaptersField,
		ProfilesDirectoryField,
	)

	copied_fields = (
		*COMMON_TARGET_FIELDS,
		PythonResolveField,
		EnvironmentField,
		RequiredAdaptersField,
		ProfilesDirectoryField,
	)
	moved_fields = (Dependencies,)
