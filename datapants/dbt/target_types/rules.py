import itertools
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping

from pants.core.target_types import FileTarget
from pants.engine.fs import PathGlobs, Paths
from pants.engine.rules import Get, MultiGet, collect_rules, rule
from pants.engine.target import (
	Dependencies,
	GeneratedTargets,
	GenerateTargetsRequest,
	OverridesField,
	SourcesField,
	Target,
	Targets,
)
from pants.engine.unions import UnionRule

from ..common_rules import DbtProjectSpec
from .config import DbtConfig
from .doc import DbtDoc
from .model import DbtModel
from .project import DbtProjectTargetGenerator
from .third_party_package import DbtThirdPartyPackage

SPEC_KEY_TARGET_MAPPING = {
	DbtModel: "model-paths",
	FileTarget: "seed-paths",
}


@dataclass(frozen=True)
class ConstructTargetsInPathRequest:
	target_generator: DbtProjectTargetGenerator
	dirpaths: tuple[str, ...]
	target_types: tuple[type[Target]]


def _filter_overrides(overrides: Mapping[str, Any], target_type: type[Target]) -> Dict[str, Any]:
	return {
		field.alias: overrides[field.alias]
		for field in target_type.core_fields
		if (not isinstance(field, SourcesField)) and field.alias in overrides
	}


@rule
async def construct_targets_in_path(request: ConstructTargetsInPathRequest) -> Targets:
	"""Generates the targets for each specified target type at the subdirectory
	supplied by dirpaths."""
	paths_by_target_type = await MultiGet(
		Get(
			Paths,
			PathGlobs(
				os.path.join(request.target_generator.address.spec_path, dirpath, "**", f"*.{ext}")
				for dirpath in request.dirpaths
				for ext in target_type.expected_file_extensions
			),
		)
		for target_type in request.target_types
	)
	return Targets(
		Target(
			{
				"source": os.path.basename(fn),
				Dependencies.alias: request.target_generator[Dependencies].value,
				**_filter_overrides(request.target_generator[OverridesField].value[fn], target_type),
			},
			request.target_generator.address.create_generated(
				os.path.relpath(fn, request.target_generator.address.spec_path)
			),
		)
		for target_type, paths in zip(request.target_types, paths_by_target_type)
		for fn in paths.files
	)


@dataclass(frozen=True)
class GenerateDbtTargetsRequest(GenerateTargetsRequest):
	generate_from = DbtProjectTargetGenerator


@rule
async def generate_dbt_targets(request: GenerateDbtTargetsRequest) -> GeneratedTargets:
	"""Generate all of the dbt targets from the dbt project root."""
	dbt_project_spec = await Get(DbtProjectSpec, DbtProjectTargetGenerator, request.generator)
	return GeneratedTargets(
		request.generator,
		(
			*itertools.chain.from_iterable(
				await MultiGet(
					Get(
						Targets,
						ConstructTargetsInPathRequest(
							request.generator, dbt_project_spec[spec_key], (target_type, DbtConfig, DbtDoc)
						),
					)
					for target_type, spec_key in SPEC_KEY_TARGET_MAPPING.items()
				)
			),
			*(
				DbtThirdPartyPackage(
					{"spec": package_spec},
					request.template_address.create_generated(
						DbtThirdPartyPackage.construct_name_from_spec(package_spec)
					),
				)
				for package_spec in dbt_project_spec.packages
				if DbtThirdPartyPackage.is_third_party_package_spec(package_spec)
			),
		),
	)


def rules():
	return (*collect_rules(), UnionRule(GenerateTargetsRequest, GenerateDbtTargetsRequest))
