import collections.abc
import os
from dataclasses import dataclass
from typing import Any

import yaml
from pants.engine.fs import Digest, DigestContents, GlobMatchErrorBehavior, PathGlobs, Snapshot
from pants.engine.rules import Get, collect_rules, rule
from pants.util.frozendict import FrozenDict
from pants.version import Version

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
	valid_package_files = ["packages.yml"]
	if Version(loaded_contents["version"]) >= "1.6":
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
