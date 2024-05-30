from __future__ import annotations

import collections.abc
import os
from dataclasses import dataclass
from typing import Any, Iterable

import yaml
from packaging.specifiers import SpecifierSet
from packaging.version import Version
from pants.engine.fs import Digest, DigestContents, GlobMatchErrorBehavior, PathGlobs, Snapshot
from pants.engine.rules import Get, collect_rules, rule
from pants.util.frozendict import FrozenDict

from .target_types import DbtProjectTargetGenerator


@dataclass(frozen=True)
class DbtProjectSpec(collections.abc.Mapping):
	project_spec: FrozenDict[str, Any]
	digest: Digest
	packages: tuple[FrozenDict[str, Any], ...] | None

	def __getitem__(self, key: str) -> Any:
		return self.project_spec[key]

	def __len__(self) -> int:
		return len(self.project_spec)

	def __iter__(self) -> collections.abc.Iterator:
		return iter(self.project_spec)


class InvalidDbtProject(Exception):
	"""An exception to use whenever the dbt project encompassed by a `dbt_project`
	target is invalid."""

	def __init__(self, message: str, *, project_name: str | None) -> None:
		super().__init__(f"Project {project_name} is invalid due to: {message}" if project_name else message)


def _safe_load_frozendict(contents: str | bytes) -> FrozenDict[str, Any]:
	return FrozenDict.deep_freeze(yaml.safe_load(contents))


@dataclass(frozen=True)
class SpecifierRange:
	min_version: str | None = None
	min_inclusive: bool = False
	max_version: str | None = None
	max_inclusive: bool = False

	def test_mins(self, versions: Iterable[str]) -> bool:
		print("Testing mins:", versions)
		base_version = Version(self.min_version)
		func = base_version.__le__ if self.min_inclusive else base_version.__lt__
		result = all(func(Version(v)) for v in versions)
		print("Result:", result)
		return result

	def test_maxs(self, versions: Iterable[str]) -> bool:
		print("Testing maxs:", versions)
		base_version = Version(self.max_version)
		func = base_version.__ge__ if self.max_inclusive else base_version.__gt__
		result = all(func(Version(v)) for v in versions)
		print("Result:", result)
		return result

	def is_subset(self, specs: SpecifierSet) -> bool:
		"""Tests whether the SpecifierSet `specs` is a subset of this specifier
		range."""
		print("Min version:", self.min_version, "Inclusive:", self.min_inclusive)
		print("Max version:", self.max_version, "Inclusive:", self.max_inclusive)
		print("Specs:", specs, "operators:", [spec.operator for spec in specs], "versions:", [spec.version for spec in specs])
		return (
			self.min_version is None
			or (
				bool(min_specs := [spec.version for spec in specs if spec.operator in {"==", ">", ">=", "~="}])
				and self.test_mins(min_specs)
			)
		) and (
			self.max_version is None
			or (  # TODO: ~= impl is not entirely correct but fine for now
				bool(max_specs := [spec.version for spec in specs if spec.operator in {"==", "<", "<=", "~="}])
				and self.test_maxs(max_specs)
			)
		)


MIN_DEPENDENCIES_YML_VERSION = SpecifierRange(min_version="1.6", min_inclusive=True)


@rule
async def load_project_spec_for_target_generator(target_generator: DbtProjectTargetGenerator) -> DbtProjectSpec:
	"""Loads the contents of the `dbt_project.yml` for a `dbt_project` target."""
	dbt_project_yml_digest = await Get(
		Digest,
		PathGlobs(
			[os.path.join(target_generator.address.spec_path, "dbt_project.yml")],
			glob_match_error_behavior=GlobMatchErrorBehavior.error,
		),
	)
	dbt_project_yml_contents = await Get(DigestContents, Digest, dbt_project_yml_digest)
	loaded_contents = _safe_load_frozendict(dbt_project_yml_contents[0].content)
	requires_dbt_version = SpecifierSet(
		loaded_contents["requires-dbt-version"]
		if isinstance(loaded_contents["requires-dbt-version"], str)
		else ",".join(loaded_contents["requires-dbt-version"])
	)
	valid_package_files = ["packages.yml"]
	if MIN_DEPENDENCIES_YML_VERSION.is_subset(requires_dbt_version):
		valid_package_files.append("dependencies.yml")
	packages_snapshot = await Get(
		Snapshot,
		PathGlobs(
			os.path.join(target_generator.address.spec_path, package_file) for package_file in valid_package_files
		),
	)
	if len(packages_snapshot.files) > 1:
		raise InvalidDbtProject(
			"Multiple packages files found - please only pass one of `packages.yml` or `dependencies.yml`.",
			project_name=loaded_contents.get("name"),
		)
	packages = None
	if packages_snapshot.files:
		packages_contents = await Get(DigestContents, Digest, packages_snapshot.digest)
		loaded_package_contents = _safe_load_frozendict(packages_contents[0].content)
		try:
			packages = loaded_package_contents["packages"]
		except KeyError as ke:
			raise InvalidDbtProject(
				"Invalid packages file without `packages` header.", project_name=loaded_contents.get("name")
			) from ke
	return DbtProjectSpec(loaded_contents, dbt_project_yml_digest, packages)


@dataclass(frozen=True)
class HydratedDbtProject:
	project_spec: FrozenDict[str, Any]
	hydrated_project: Snapshot


@rule
async def hydrate_dbt_project(target: DbtProjectTargetGenerator) -> HydratedDbtProject:
	""""""


def rules():
	return collect_rules()
