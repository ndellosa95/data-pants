from __future__ import annotations

import os

from pants.backend.python.target_types import PythonResolveField
from pants.core.util_rules.environments import EnvironmentField
from pants.engine.addresses import Address
from pants.engine.target import (
	COMMON_TARGET_FIELDS,
	Dependencies,
	InvalidFieldException,
	OverridesField,
	SingleSourceField,
	StringSequenceField,
	Target,
	TargetGenerator,
)
from pants.util.strutil import softwrap


class HardcodedSingleSourceField(SingleSourceField):
	required = False

	@classmethod
	def compute_value(cls, raw_value: str | None, address: Address) -> str:
		result = super().compute_value(raw_value, address)
		if result is None or os.path.basename(result) not in (valid_choices := cls.valid_choices or [cls.default]):
			valid_choices = [repr(vc) for vc in valid_choices]
			equal_str = (
				valid_choices[0]
				if len(valid_choices) == 1
				else f"one of {', '.join(valid_choices[:-1])} or {valid_choices[-1]}"
			)
			raise InvalidFieldException(f"Field `{cls.alias}` at address {address} must equal {equal_str}")
		return result


class ProjectFileField(HardcodedSingleSourceField):
	alias = "project_file"
	help = "The path to the `dbt_project.yml` file associated with this project."
	default = "dbt_project.yml"


class ProfilesFileField(HardcodedSingleSourceField):
	alias = "profiles_file"
	help = "The path to the `profiles.yml` associated with this project."
	default = "profiles.yml"


class PackagesFileField(HardcodedSingleSourceField):
	alias = "packages_file"
	help = "The path to the packages file for loading third-party packages for this project."
	valid_choices = ("packages.yml", "dependencies.yml", "package-lock.yml")
	default = "packages.yml"


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
		ProjectFileField,
		ProfilesFileField,
		PackagesFileField,
		PythonResolveField,
		RequiredAdaptersField,
	)

	copied_fields = (
		*COMMON_TARGET_FIELDS,
		PythonResolveField,
		EnvironmentField,
		RequiredAdaptersField,
		ProfilesFileField,
	)
	moved_fields = (Dependencies,)
