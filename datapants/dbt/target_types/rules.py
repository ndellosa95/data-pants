import itertools
import logging
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
	InvalidFieldException
)
from pants.engine.unions import UnionRule
from pants.util.strutil import comma_separated_list

from ..common_rules import DbtProjectSpec
from .config import DbtConfig, DbtConfigSourceField
from .doc import DbtDoc, DbtDocSourceField
from .model import DbtModel, DbtModelSourceField
from .project import DbtProjectTargetGenerator, ProjectFileField
from .third_party_package import DbtThirdPartyPackage, DbtThirdPartyPackageSpec

LOGGER = logging.getLogger(__file__)

EXPECTED_EXTENSIONS_MAPPING = {
	DbtModel: DbtModelSourceField.expected_file_extensions,
	DbtConfig: DbtConfigSourceField.expected_file_extensions,
	DbtDoc: DbtDocSourceField.expected_file_extensions,
	FileTarget: (".csv",),
}

SPEC_KEY_TARGET_MAPPING = {
	DbtModel: "model-paths",
	FileTarget: "seed-paths",
}


@dataclass(frozen=True)
class ConstructTargetsInPathRequest:
	target_generator: DbtProjectTargetGenerator
	dirpaths: tuple[str, ...]
	target_types: tuple[type[Target]]


def _validate_overrides(overrides: OverridesField, filename: str) -> Dict[str, Any]:
	if overrides.value and (overrides_for_file := overrides.value.get(filename)):
		if "sources" in overrides_for_file:
			raise InvalidFieldException("Cannot override field `sources`", description_of_origin=f"Target at address {overrides.address}")
		return overrides_for_file
	return {}


@rule
async def construct_targets_in_path(request: ConstructTargetsInPathRequest) -> Targets:
	"""Generates the targets for each specified target type at the subdirectory
	supplied by dirpaths."""
	LOGGER.debug(
		f"Checking path(s) {comma_separated_list(request.dirpaths)} for "
		f"{comma_separated_list(tt.alias for tt in request.target_types)} targets... "
	)
	paths_by_target_type = await MultiGet(
		Get(
			Paths,
			PathGlobs(
				os.path.join(os.path.dirname(request.target_generator[ProjectFileField].file_path), dirpath, "**", f"*{ext}")
				for dirpath in request.dirpaths
				for ext in EXPECTED_EXTENSIONS_MAPPING[target_type]
			),
		)
		for target_type in request.target_types
	)
	return Targets(
		target_type(
			{
				"source": os.path.basename(fn),
				Dependencies.alias: request.target_generator[Dependencies].value,
				**_validate_overrides(request.target_generator[OverridesField], fn),
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
					{DbtThirdPartyPackageSpec.alias: package_spec},
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
