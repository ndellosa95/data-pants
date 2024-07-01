import collections.abc
from typing import Any, Mapping
from urllib.parse import urlparse

from pants.engine.addresses import Address
from pants.engine.target import COMMON_TARGET_FIELDS, Field, InvalidFieldException, InvalidFieldTypeException, Target
from pants.util.frozendict import FrozenDict


class DbtThirdPartyPackageSpec(Field):
	alias = "specification"
	help = "The specification that makes up the entry for this third-party package."
	required = True

	@classmethod
	def compute_value(cls, raw_value: Any, address: Address) -> FrozenDict[str, Any]:
		value_or_default = super().compute_value(raw_value, address)
		if not isinstance(value_or_default, collections.abc.Mapping):
			raise InvalidFieldTypeException(
				address,
				cls.alias,
				raw_value,
				expected_type="Mapping[str, Any]",
				description_of_origin=f"The target generator at `{address.spec_path}`"
				if address.is_generated_target
				else None,
			)
		return FrozenDict.deep_freeze(value_or_default)

	@property
	def package_name(self) -> str:
		return DbtThirdPartyPackage.construct_name_from_spec(self.value)


class DbtThirdPartyPackage(Target):
	alias = "dbt_third_party_package"
	help = "A target representing a third-party package for a dbt project."
	core_fields = (*COMMON_TARGET_FIELDS, DbtThirdPartyPackageSpec)

	@staticmethod
	def construct_name_from_spec(spec: Mapping[str, Any]) -> str:
		if "tarball" in spec:
			try:
				return spec["name"]
			except KeyError as ke:
				raise InvalidFieldException(
					"Tarball packages require a `name` field", description_of_origin=f"Package spec {spec}"
				) from ke
		if "package" in spec:
			package: str = spec["package"]
			return package[package.rindex("/") + 1 :]
		return urlparse(spec["git"]).path[1:-4]

	@staticmethod
	def is_third_party_package_spec(package_spec: Mapping[str, Any]) -> bool:
		return any(k in package_spec for k in ("package", "git", "tarball"))
