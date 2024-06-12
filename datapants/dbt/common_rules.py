from __future__ import annotations

import collections.abc
import json
import logging
import os
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass, field, replace
from textwrap import dedent
from typing import Any, Iterable, Iterator

import yaml
from packaging.specifiers import Specifier, SpecifierSet
from packaging.version import Version
from pants.backend.python.subsystems.setup import PythonSetup
from pants.backend.python.target_types import ConsoleScript, PythonResolveField
from pants.backend.python.util_rules.pex import PexRequest, PexRequirements, Resolve, VenvPex, VenvPexProcess
from pants.core.target_types import FileSourceField
from pants.core.util_rules.system_binaries import ChmodBinary, CpBinary, MkdirBinary
from pants.engine.addresses import Address
from pants.engine.fs import (
	EMPTY_DIGEST,
	CreateDigest,
	Digest,
	DigestContents,
	FileContent,
	MergeDigests,
	PathGlobs,
	Snapshot,
)
from pants.engine.process import Process, ProcessResult
from pants.engine.rules import Get, MultiGet, collect_rules, rule
from pants.engine.target import (
	AllTargets,
	Dependencies,
	DependenciesRequest,
	HydratedSources,
	HydrateSourcesRequest,
	SingleSourceField,
	SourcesField,
	Targets,
)
from pants.util.frozendict import FrozenDict
from pants.util.memo import memoized_property
from pants.util.strutil import shell_quote

from .target_types import DbtSourceField
from .target_types.project import (
	DbtProjectTargetGenerator,
	PackagesFileField,
	ProfileTargetField,
	ProjectFileField,
	RequiredAdaptersField,
	RequiredEnvVarsField,
)
from .target_types.third_party_package import DbtThirdPartyPackageSpec

LOGGER = logging.getLogger(__file__)


def ensure_generated_address(address: Address, activity: str, warn_only: bool = True) -> bool:
	log_msg = LOGGER.warning if warn_only else LOGGER.error
	if not address.is_generated_target:
		log_msg(
			f"Unable to {activity} for dbt target at address `{address}`, which exists independent of any "
			"dbt project."
		)
		return False
	return True


@dataclass(frozen=True)
class DbtProjectSpec(collections.abc.Mapping):
	project_spec: FrozenDict[str, Any]
	digest: Digest
	packages: tuple[FrozenDict[str, Any], ...]

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

	def __str__(self) -> str:
		parts = []
		if self.min_version:
			parts.append((">=" if self.min_inclusive else ">") + self.min_version)
		if self.max_version:
			parts.append(("<=" if self.max_inclusive else "<") + self.max_version)
		return ",".join(parts)

	@staticmethod
	def _convert_spec_version(spec: Specifier) -> Version:
		return Version(spec.version.replace("*", "0"))

	def test_mins(self, specs: Iterable[Specifier]) -> bool:
		base_version = Version(self.min_version)
		return all(
			(base_version.__le__ if self.min_inclusive or spec.operator == ">" else base_version.__lt__)(
				self._convert_spec_version(spec)
			)
			for spec in specs
		)

	def test_maxs(self, specs: Iterable[Specifier]) -> bool:
		def _fix_compatible(s: Specifier) -> Version:
			v = self._convert_spec_version(s)
			return Version(".".join(map(str, [*v.release[:-2], v.release[-2] + 1]))) if s.operator == "~=" else v

		base_version = Version(self.max_version)
		return all(
			(base_version.__ge__ if self.max_inclusive or spec.operator == "<" else base_version.__gt__)(
				_fix_compatible(spec)
			)
			for spec in specs
		)

	def is_subset(self, specs: SpecifierSet) -> bool:
		"""Tests whether the SpecifierSet `specs` is a subset of this specifier
		range."""
		return (
			self.min_version is None
			or (
				bool(min_specs := [spec for spec in specs if spec.operator in {"==", ">", ">=", "~="}])
				and self.test_mins(min_specs)
			)
		) and (
			self.max_version is None
			or (
				bool(max_specs := [spec for spec in specs if spec.operator in {"==", "<", "<=", "~="}])
				and self.test_maxs(max_specs)
			)
		)


MIN_DEPENDENCIES_YML_VERSION = SpecifierRange(min_version="1.6", min_inclusive=True)
MIN_LOCKFILE_YML_VERSION = SpecifierRange(min_version="1.7", min_inclusive=True)


@rule
async def load_project_spec_for_target_generator(target_generator: DbtProjectTargetGenerator) -> DbtProjectSpec:
	"""Loads the contents of the `dbt_project.yml` for a `dbt_project` target."""
	dbt_project_yml_sources = await Get(HydratedSources, HydrateSourcesRequest(target_generator[ProjectFileField]))
	if not dbt_project_yml_sources.snapshot.files:
		raise InvalidDbtProject(
			f"No `dbt_project.yml` found for `{target_generator.alias}` target at address {target_generator.address}"
		)
	dbt_project_yml_contents = await Get(DigestContents, Digest, dbt_project_yml_sources.snapshot.digest)
	loaded_contents = _safe_load_frozendict(dbt_project_yml_contents[0].content)
	if (
		package_file_basename := os.path.basename(target_generator[PackagesFileField].value)
	) != "packages.yml" and not (
		version_range := MIN_LOCKFILE_YML_VERSION
		if package_file_basename == "package-lock.yml"
		else MIN_DEPENDENCIES_YML_VERSION
	).is_subset(
		SpecifierSet(
			loaded_contents["requires-dbt-version"]
			if isinstance(loaded_contents["requires-dbt-version"], str)
			else ",".join(loaded_contents["requires-dbt-version"])
		)
	):
		raise InvalidDbtProject(
			f"{package_file_basename} is only valid for dbt versions {version_range}",
			project_name=loaded_contents.get("name"),
		)

	packages_sources = await Get(HydratedSources, HydrateSourcesRequest(target_generator[PackagesFileField]))
	packages_contents = await Get(DigestContents, Digest, packages_sources.snapshot.digest)
	loaded_package_contents = _safe_load_frozendict(packages_contents[0].content)
	try:
		packages = loaded_package_contents["packages"]
	except KeyError as ke:
		raise InvalidDbtProject(
			"Invalid packages file without `packages` header.", project_name=loaded_contents.get("name")
		) from ke
	return DbtProjectSpec(loaded_contents, dbt_project_yml_sources.snapshot.digest, packages)


@dataclass(frozen=True)
class HydrateDbtEnvVarsRequest:
	field: RequiredEnvVarsField


class DbtEnvVars(FrozenDict[str, str]):
	pass


@rule
async def hydrate_dbt_env_vars(request: HydrateDbtEnvVarsRequest) -> DbtEnvVars:
	if isinstance(request.field.value, collections.abc.Mapping):
		return DbtEnvVars(request.field.value)
	env_var_filepath = os.path.relpath(
		os.path.join(request.field.address.spec_path, request.field.value), request.field.address.spec_path
	)
	env_file_digest = await Get(Digest, PathGlobs([env_var_filepath]))
	if env_file_digest is EMPTY_DIGEST:
		raise RuntimeError(f"Cannot locate file environment file at path `{env_var_filepath}`.")
	env_var_file_contents = await Get(DigestContents, Digest, env_file_digest)
	return DbtEnvVars(
		RequiredEnvVarsField.parse_string(line) for line in env_var_file_contents[0].content.decode().splitlines()
	)


@dataclass(frozen=True)
class DbtCli:
	pex: VenvPex
	target: DbtProjectTargetGenerator
	project_name: str
	profile_name: str
	target_path_in_project_dir: str

	@property
	def profile_target(self) -> str | None:
		return self.target[ProfileTargetField].value

	@property
	def target_path(self) -> str:
		return os.path.join(self.target.project_dir, self.target_path_in_project_dir)

	@property
	def cached_target_path(self) -> str:
		return (
			f"__{self.project_name}_{self.profile_name}_{self.profile_target}_{self.target_path_in_project_dir}_"
			+ "append_only_target_cache__"
		)

	def make_args(self, *args: str) -> tuple[str, ...]:
		added_args = [f"--project-dir={self.target.project_dir}", f"--profiles-dir={self.target.profiles_dir}"]
		if self.profile_target:
			added_args.append(f"--target={self.profile_target}")
		return (*args, *added_args)

	@property
	def append_only_caches(self) -> dict[str, str]:
		return {f"{self.project_name}_{self.profile_name}_{self.profile_target}".lower(): self.cached_target_path}


@rule
async def compose_dbt_cli(target: DbtProjectTargetGenerator, python_setup: PythonSetup) -> DbtCli:
	"""Generates a pex for the dbt CLI for a single dbt project."""
	project_spec, pex = await MultiGet(
		Get(DbtProjectSpec, DbtProjectTargetGenerator, target),
		Get(
			VenvPex,
			PexRequest(
				output_filename="dbt.pex",
				internal_only=True,
				main=ConsoleScript("dbt"),
				requirements=PexRequirements(
					["dbt-core", *target[RequiredAdaptersField].value],
					from_superset=Resolve(
						target[PythonResolveField].normalized_value(python_setup), use_entire_lockfile=False
					),
				),
			),
		),
	)
	return DbtCli(
		pex,
		target,
		project_spec["name"],
		project_spec["profile"],
		project_spec.get("target-path", "target"),
	)


@dataclass(frozen=True)
class DbtCliCommandRequest:
	cli: DbtCli
	digest: Digest
	argv: tuple[str, ...]
	description: str = field(compare=False)
	output_directories: tuple[str, ...] | None = None
	output_files: tuple[str, ...] | None = None


@rule
async def get_dbt_cli_command_process(
	request: DbtCliCommandRequest,
	mkdir: MkdirBinary,
	cp: CpBinary,
	chmod: ChmodBinary,
) -> Process:
	"""Runs a dbt CLI command."""
	env_vars = await Get(DbtEnvVars, HydrateDbtEnvVarsRequest(request.cli.target[RequiredEnvVarsField]))
	pex_process = await Get(
		Process,
		VenvPexProcess(
			request.cli.pex,
			argv=request.cli.make_args(*request.argv),
			description=request.description,
			input_digest=request.digest,
			output_directories=request.output_directories,
			output_files=(*(request.output_files or ()), "!__dbt_runner.sh"),
			extra_env=env_vars,
			append_only_caches=request.cli.append_only_caches,
		),
	)
	script_runner_content = f"""\
		if [ ! -d {request.cli.target_path} ]; then
			{mkdir.path} -p {request.cli.target_path} > /dev/null 2>&1
			if [ -d {request.cli.cached_target_path} ]; then
				{cp.path} -r {os.path.join(request.cli.cached_target_path, "**")} {request.cli.target_path} > /dev/null 2>&1
			fi
		fi

		{chmod.path} -R 666 {request.cli.target_path} > /dev/null 2>&1		
		{' '.join((shell_quote(arg) for arg in pex_process.argv))}
		EXIT_CODE=$?

		if [ -d {request.cli.target_path} ]; then
			{cp.path} -r {os.path.join(request.cli.target_path, "**")} {request.cli.cached_target_path} > /dev/null 2>&1
		fi
		exit $EXIT_CODE
	"""
	script_runner_digest = await Get(
		Digest,
		CreateDigest([FileContent("__dbt_runner.sh", dedent(script_runner_content).encode(), is_executable=True)]),
	)
	new_process_digest = await Get(Digest, MergeDigests([script_runner_digest, pex_process.input_digest]))
	return replace(pex_process, argv=("__dbt_runner.sh",), input_digest=new_process_digest)


@dataclass(frozen=True)
class HydratedDbtProject:
	cli: DbtCli
	hydrated_project: Snapshot

	def parse_command(self) -> DbtCliCommandRequest:
		return DbtCliCommandRequest(
			self.cli,
			self.hydrated_project.digest,
			("parse",),
			description="Parsing dbt project",
			output_files=(os.path.join(self.cli.target_path, "manifest.json"),),
		)

	def compile_command(self) -> DbtCliCommandRequest:
		return DbtCliCommandRequest(
			self.cli,
			self.hydrated_project.digest,
			("compile",),
			description="Compiling dbt project",
			output_directories=(".",),
		)


@rule
async def hydrate_dbt_project(target: DbtProjectTargetGenerator) -> HydratedDbtProject:
	"""Returns a fully hydrated dbt project including dependencies and the pex for
	the dbt CLI."""
	all_project_deps, dbt_cli = await MultiGet(
		Get(Targets, DependenciesRequest(target[Dependencies])), Get(DbtCli, DbtProjectTargetGenerator, target)
	)
	hydrated_sources = await MultiGet(
		Get(HydratedSources, HydrateSourcesRequest(tgt[SourcesField]))
		for tgt in all_project_deps
		if tgt.has_field(SourcesField)
	)
	merged_sources = await Get(Digest, MergeDigests(source.snapshot.digest for source in hydrated_sources))
	deps_process = await Get(
		Process,
		DbtCliCommandRequest(
			dbt_cli, merged_sources, ("deps",), description="Hydrating dbt dependencies", output_directories=(".",)
		),
	)
	deps_result = await Get(ProcessResult, Process, deps_process)
	return HydratedDbtProject(dbt_cli, await Get(Snapshot, Digest, deps_result.output_digest))


MINIMUM_NODE_SET = frozenset(["original_file_path", "unique_id"])


@dataclass(frozen=True)
class DbtManifest:
	"""Dataclass representing a generated dbt manifest for a single dbt
	project."""

	project_target: DbtProjectTargetGenerator
	content: FrozenDict[str, Any]
	digest: Digest

	@memoized_property
	def unique_id_to_node_mapping(self) -> FrozenDict[str, FrozenDict[str, Any]]:
		def traverse(root: FrozenDict[str, Any]) -> Iterator[tuple[str, FrozenDict[str, Any]]]:
			for v in root.values():
				if not isinstance(v, FrozenDict):
					continue
				if MINIMUM_NODE_SET.issubset(v.keys()):
					yield v["unique_id"], v
				else:
					yield from traverse(v)

		return FrozenDict(traverse(self.content))


@rule
async def parse_dbt_project(target: DbtProjectTargetGenerator) -> DbtManifest:
	hydrated_project = await Get(HydratedDbtProject, DbtProjectTargetGenerator, target)
	parse_process = await Get(Process, DbtCliCommandRequest, hydrated_project.parse_command())
	parse_result = await Get(ProcessResult, Process, parse_process)
	manifest_digest_contents = await Get(DigestContents, Digest, parse_result.output_digest)
	return DbtManifest(
		target, FrozenDict.deep_freeze(json.loads(manifest_digest_contents[0].content)), parse_result.output_digest
	)


@dataclass(frozen=True)
class AddressToDbtUniqueIdMapping(collections.abc.Mapping):
	manifest: DbtManifest
	unique_ids_per_address: FrozenDict[Address, frozenset[str]]

	def get_nodes_for_address(self, address: Address) -> tuple[FrozenDict[str, Any], ...]:
		return tuple(
			self.manifest.unique_id_to_node_mapping[unique_id] for unique_id in self.unique_ids_per_address[address]
		)

	@memoized_property
	def unique_id_to_address_mapping(self) -> FrozenDict[str, Address]:
		return FrozenDict(
			{
				unique_id: address
				for address, unique_ids in self.unique_ids_per_address.items()
				for unique_id in unique_ids
			}
		)

	def __getitem__(self, k: Address) -> frozenset[str]:
		return self.unique_ids_per_address[k]

	def __len__(self) -> int:
		return len(self.unique_ids_per_address)

	def __iter__(self) -> Iterator[Address]:
		return iter(self.unique_ids_per_address)


@rule
async def map_addresses_to_unique_ids(manifest: DbtManifest, all_targets: AllTargets) -> AddressToDbtUniqueIdMapping:
	address_by_file = {
		os.path.relpath(tgt[SingleSourceField].file_path, manifest.project_target.project_dir): tgt.address
		for tgt in all_targets
		if tgt.has_field(DbtSourceField) or tgt.has_field(FileSourceField)
	}
	address_by_package = {
		tgt[DbtThirdPartyPackageSpec].package_name: tgt.address
		for tgt in all_targets
		if tgt.has_field(DbtThirdPartyPackageSpec)
	}
	unique_ids_per_address = defaultdict(set)
	for unique_id, node in manifest.unique_id_to_node_mapping.items():
		with suppress(KeyError):
			unique_ids_per_address[
				address_by_file.get(node["original_file_path"]) or address_by_package[node["package_name"]]
			].add(unique_id)
	return AddressToDbtUniqueIdMapping(
		manifest, FrozenDict({address: frozenset(unique_ids) for address, unique_ids in unique_ids_per_address.items()})
	)


def rules():
	return collect_rules()
