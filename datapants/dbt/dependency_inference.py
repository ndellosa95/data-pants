import logging
from dataclasses import dataclass

from pants.backend.python.subsystems.setup import PythonSetup
from pants.backend.python.target_types import PythonRequirementResolveField, PythonRequirementsField, PythonResolveField
from pants.engine.addresses import Addresses
from pants.engine.internals.graph import Owners, OwnersRequest
from pants.engine.rules import Get, collect_rules, rule
from pants.engine.target import (
	AllTargets,
	Dependencies,
	FieldSet,
	InferDependenciesRequest,
	InferredDependencies,
	UnexpandedTargets,
)
from pants.engine.unions import UnionRule

from .common_rules import AddressToDbtUniqueIdMapping, DbtManifest, DbtProjectSpec, ensure_generated_address
from .target_types import DbtProjectTargetGenerator, DbtSourceField
from .target_types.macro import DbtMacroSourceField
from .target_types.model import DbtModelSourceField
from .target_types.project import RequiredAdaptersField

LOGGER = logging.getLogger(__file__)


@dataclass(frozen=True)
class DbtProjectDependencyInferenceFieldSet(FieldSet):
	required_fields = (Dependencies, PythonResolveField, RequiredAdaptersField)

	python_resolve: PythonResolveField
	required_adapters: RequiredAdaptersField


class InferDbtProjectDependenciesRequest(InferDependenciesRequest):
	infer_from = DbtProjectDependencyInferenceFieldSet


@rule
async def infer_dbt_project_dependencies(
	request: InferDbtProjectDependenciesRequest, all_targets: AllTargets, python_setup: PythonSetup
) -> InferredDependencies:
	"""Infer the `python_requirement` dependencies for the dbt project."""
	adapter_set = {"dbt-core", *request.field_set.required_adapters.value}
	resolve = request.field_set.python_resolve.normalized_value(python_setup)
	return InferredDependencies(
		tgt.address
		for tgt in all_targets
		if tgt.has_fields((PythonRequirementsField, PythonRequirementResolveField))
		and tgt[PythonRequirementResolveField].normalized_value(python_setup) == resolve
		and tgt[PythonRequirementsField].value[0].project_name in adapter_set
	)


@dataclass(frozen=True)
class DbtComponentDependencyInferenceFieldSet(FieldSet):
	required_fields = (DbtSourceField, Dependencies)

	source: DbtSourceField
	dependencies: Dependencies


class InferDbtComponentDependenciesRequest(InferDependenciesRequest):
	infer_from = DbtComponentDependencyInferenceFieldSet

	@property
	def is_model_request(self) -> bool:
		return isinstance(self.field_set.source, DbtModelSourceField)

	@property
	def is_macro_request(self) -> bool:
		return isinstance(self.field_set.source, DbtMacroSourceField)


@rule
async def infer_dbt_component_dependencies(request: InferDbtComponentDependenciesRequest) -> InferredDependencies:
	"""Infer the dependency relationships between each individual dbt
	component."""
	if not ensure_generated_address(request.field_set.address, "infer dependencies"):
		return InferredDependencies([])
	dbt_project_targets = await Get(
		UnexpandedTargets, Addresses([request.field_set.address.maybe_convert_to_target_generator()])
	)
	dbt_project_target: DbtProjectTargetGenerator = dbt_project_targets.expect_single()
	dbt_manifest = await Get(DbtManifest, DbtProjectTargetGenerator, dbt_project_target)
	address_to_unique_id_mapping = await Get(AddressToDbtUniqueIdMapping, DbtManifest, dbt_manifest)
	if request.field_set.address not in address_to_unique_id_mapping:
		return InferredDependencies([])

	if request.is_macro_request:
		deps = {
			address_to_unique_id_mapping.unique_id_to_address_mapping[parent_macro_id]
			for unique_id in address_to_unique_id_mapping[request.field_set.address]
			for parent_macro_id in dbt_manifest.unique_id_to_node_mapping[unique_id]["depends_on"]["macros"]
		}
	else:
		deps = {
			address_to_unique_id_mapping.unique_id_to_address_mapping[parent_unique_id]
			for unique_id in address_to_unique_id_mapping[request.field_set.address]
			for parent_unique_id in dbt_manifest.content["parent_map"][unique_id]
		}
		if request.is_model_request:
			deps |= {
				address_to_unique_id_mapping.unique_id_to_address_mapping[child_unique_id]
				for unique_id in address_to_unique_id_mapping[request.field_set.address]
				for child_unique_id in dbt_manifest.content["child_map"][unique_id]
				if dbt_manifest.unique_id_to_node_mapping[child_unique_id]["resource_type"] != "model"
			}
			project_spec = await Get(DbtProjectSpec, DbtProjectTargetGenerator, dbt_project_target)
			deps |= await Get(
				Owners,
				OwnersRequest(
					tuple(
						patch_path.replace(f"{project_spec['name']}://", f"{dbt_project_target.project_dir}/")
						for unique_id in address_to_unique_id_mapping[request.field_set.address]
						if (patch_path := dbt_manifest.unique_id_to_node_mapping[unique_id].get("patch_path"))
					)
				),
			)
	return InferredDependencies(deps - {request.field_set.address})


def rules():
	return (
		*collect_rules(),
		UnionRule(InferDependenciesRequest, InferDbtProjectDependenciesRequest),
		UnionRule(InferDependenciesRequest, InferDbtComponentDependenciesRequest),
	)
