import logging
from dataclasses import dataclass

from pants.backend.python.subsystems.setup import PythonSetup
from pants.backend.python.target_types import PythonRequirementResolveField, PythonRequirementsField, PythonResolveField
from pants.engine.rules import collect_rules, rule
from pants.engine.target import AllTargets, Dependencies, FieldSet, InferDependenciesRequest, InferredDependencies
from pants.engine.unions import UnionRule

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


def rules():
	return (*collect_rules(), UnionRule(InferDependenciesRequest, InferDbtProjectDependenciesRequest))
