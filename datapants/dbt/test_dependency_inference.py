import pytest
from pants.backend.python.subsystems.setup import PythonSetup
from pants.backend.python.target_types import PythonRequirementTarget, PythonResolveField
from pants.core.target_types import FileTarget
from pants.engine.addresses import Address
from pants.engine.target import AllTargets, InferredDependencies
from pants.testutil.option_util import create_subsystem
from pants.testutil.python_rule_runner import PythonRuleRunner
from pants.testutil.rule_runner import run_rule_with_mocks

from .conftest import parametrize_python_versions
from .dependency_inference import (
	DbtProjectDependencyInferenceFieldSet,
	InferDbtComponentDependenciesRequest,
	InferDbtProjectDependenciesRequest,
	infer_dbt_project_dependencies,
)
from .target_types.project import RequiredAdaptersField


def test_infer_dbt_project_dependencies() -> None:
	project_root_address = Address("fake", target_name="project_root")
	request = InferDbtProjectDependenciesRequest(
		DbtProjectDependencyInferenceFieldSet(
			project_root_address,
			PythonResolveField("fake_resolve", project_root_address),
			RequiredAdaptersField(["dbt-duckdb"], project_root_address),
		)
	)
	duckdb_address = Address("fake", target_name="duckdb")
	dbt_core_address = Address("fake", target_name="dbtcore")
	all_targets = AllTargets(
		[
			PythonRequirementTarget({"requirements": ["dbt-duckdb"]}, duckdb_address),
			PythonRequirementTarget({"requirements": ["dbt-core~=1.7.0"]}, dbt_core_address),
			PythonRequirementTarget({"requirements": ["irrelevant==1.2.3"]}, Address("fake", target_name="irrelevant")),
			FileTarget({"source": "file.txt"}, Address("fake", target_name="irrelevant2")),
		]
	)
	assert run_rule_with_mocks(
		infer_dbt_project_dependencies,
		rule_args=(
			request,
			all_targets,
			create_subsystem(
				PythonSetup,
				enable_resolves=True,
				default_resolve="fake_resolve",
				resolves={"fake_resolve": "fake_lock.txt"},
			),
		),
	) == InferredDependencies([dbt_core_address, duckdb_address])


@parametrize_python_versions
@pytest.mark.parametrize(
	argnames=("request_target_name", "inferred_deps"),
	argvalues=[
		(
			"models/marts/customers.sql",
			{"models/marts/customers.yml", "models/marts/orders.sql", "models/staging/stg_customers.sql"},
		),
		("models/marts/order_items.yml", {"models/marts/order_items.sql", "models/marts/orders.yml"}),
		("models/staging/stg_customers.yml", set()),
		# TODO: add macro, doc, and data-test test cases
	],
	ids=("Model", "Config with Cycle", "Config without Cycle"),
)
def test_infer_dbt_component_dependencies(
	sample_project_rule_runner: PythonRuleRunner, request_target_name: str, inferred_deps: set[str]
) -> None:
	root_address = Address("sample-project", target_name="root")
	assert sample_project_rule_runner.request(
		InferredDependencies,
		[
			InferDbtComponentDependenciesRequest(
				InferDbtComponentDependenciesRequest.infer_from.create(
					sample_project_rule_runner.get_target(root_address.create_generated(request_target_name))
				)
			)
		],
	) == InferredDependencies(root_address.create_generated(dep) for dep in inferred_deps)
