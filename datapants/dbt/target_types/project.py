from __future__ import annotations

import collections.abc
import os
from typing import Any

from pants.backend.python.target_types import PythonResolveField
from pants.core.util_rules.environments import EnvironmentField
from pants.engine.addresses import Address
from pants.engine.target import (
	COMMON_TARGET_FIELDS,
	AsyncFieldMixin,
	Dependencies,
	InvalidFieldException,
	InvalidFieldTypeException,
	OverridesField,
	SingleSourceField,
	StringSequenceField,
	TargetGenerator,
)
from pants.util.frozendict import FrozenDict
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


class DbtProjectOverridesField(OverridesField):
	help = "Overrides for field defaults propagated by the `dbt_project` target generator."


class RequiredAdaptersField(StringSequenceField):
	alias = "required_adapters"
	help = "All possible adapters required by this dbt project."
	required = True


class RequiredEnvVarsField(AsyncFieldMixin):
	alias = "env_vars"
	help = softwrap(
		"""Pass environment variables to the sandbox environment used to run dbt commands for this project.
		
		The value of this field can be either a dictionary, a list of strings in the format 
		`ENV_VAR_NAME=ENV_VAR_VALUE`, or a path to a file to source environment variables from."""
	)
	required = False

	@staticmethod
	def parse_string(s: str) -> tuple[str, str]:
		name, value = s.split("=", 1)
		name, (value[1:-1] if value[0] == value[-1] and value[0] in {'"', "'"} else value)

	@classmethod
	def compute_value(cls, raw_value: Any, address: Address) -> str | FrozenDict[str, str]:
		if not raw_value:
			return FrozenDict()
		if isinstance(raw_value, str):
			return raw_value
		field_type_exception = InvalidFieldTypeException(
			address,
			cls.alias,
			raw_value,
			expected_type="a dictionary, a list of strings in the format `ENV_VAR_NAME=ENV_VAR_VALUE`, or a path to a file to source environment variables from",
		)

		def validate_string(s: Any) -> str:
			if not isinstance(s, str):
				raise field_type_exception
			return s

		if isinstance(raw_value, collections.abc.Mapping):
			return FrozenDict({validate_string(k): validate_string(v) for k, v in raw_value.items()})
		if isinstance(raw_value, collections.abc.Iterable):
			return FrozenDict(cls.parse_string(validate_string(entry)) for entry in raw_value)
		raise field_type_exception


class DbtProjectTargetGenerator(TargetGenerator):
	alias = "dbt_project"
	help = softwrap(
		"""A target generator representing a single dbt project which generates targets for all of the projects underlying components."""
	)

	core_fields = (
		*COMMON_TARGET_FIELDS,
		Dependencies,
		DbtProjectOverridesField,
		EnvironmentField,
		ProfilesFileField,
		PackagesFileField,
		ProjectFileField,
		PythonResolveField,
		RequiredAdaptersField,
		RequiredEnvVarsField,
	)

	copied_fields = ()
	moved_fields = (
		*COMMON_TARGET_FIELDS,
		Dependencies,
	)

	@property
	def profiles_dir(self) -> str:
		return os.path.join(self.address.spec_path, os.path.dirname(self[ProfilesFileField].value))

	@property
	def project_dir(self) -> str:
		return os.path.join(self.address.spec_path, os.path.dirname(self[ProjectFileField].value))
