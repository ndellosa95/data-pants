from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
import yaml
from packaging.specifiers import SpecifierSet
from pants.engine.addresses import Address
from pants.engine.environment import EnvironmentName
from pants.engine.fs import EMPTY_DIGEST
from pants.engine.internals.scheduler import ExecutionError
from pants.testutil.rule_runner import QueryRule, RuleRunner
from pants.util.frozendict import FrozenDict

from .common_rules import DbtEnvVars, DbtProjectSpec, HydrateDbtEnvVarsRequest, InvalidDbtProject, SpecifierRange
from .common_rules import rules as common_rules
from .target_types import DbtProjectTargetGenerator
from .target_types.project import RequiredEnvVarsField


@pytest.mark.parametrize(
	argnames=("min_version", "min_inclusive", "min_req", "meets_min_req"),
	argvalues=[
		(None, False, ">1.0.0", True),
		("1.0.0", True, ">=1.0.0", True),
		("1.0.0", False, ">=1.0.0", False),
		("1.0.0", True, ">=1.0.1", True),
		("1.0.0", False, ">=1.0.1", True),
		("1.0.0", True, ">=0.9.9", False),
		("1.0.0", False, ">=0.9.9", False),
		("1.0.0", True, ">1.0.0", True),
		("1.0.0", False, ">1.0.0", True),
		("1.0.0", True, ">1.0.1", True),
		("1.0.0", False, ">1.0.1", True),
		("1.0.0", True, ">0.9.9", False),
		("1.0.0", False, ">0.9.9", False),
		("1.0.0", True, None, False),
	],
)
@pytest.mark.parametrize(
	argnames=("max_version", "max_inclusive", "max_req", "meets_max_req"),
	argvalues=[
		(None, False, "<2.0.0", True),
		("2.0.0", True, "<=2.0.0", True),
		("2.0.0", False, "<=2.0.0", False),
		("2.0.0", True, "<=2.0.1", False),
		("2.0.0", False, "<=2.0.1", False),
		("2.0.0", True, "<=1.9.9", True),
		("2.0.0", False, "<=1.9.9", True),
		("2.0.0", True, "<2.0.0", True),
		("2.0.0", False, "<2.0.0", True),
		("2.0.0", True, "<2.0.1", False),
		("2.0.0", False, "<2.0.1", False),
		("2.0.0", True, "<1.9.9", True),
		("2.0.0", False, "<1.9.9", True),
		("2.0.0", True, None, False),
	],
)
def test_specifier_range_is_subset(
	min_version: str | None,
	min_inclusive: bool,
	min_req: str | None,
	meets_min_req: bool,
	max_version: str | None,
	max_inclusive: bool,
	max_req: str | None,
	meets_max_req: bool,
) -> None:
	spec_range = SpecifierRange(min_version, min_inclusive, max_version, max_inclusive)
	expected = meets_min_req and meets_max_req
	spec_set = SpecifierSet(",".join(filter(bool, [min_req, max_req])))
	assert spec_range.is_subset(spec_set) == expected


@pytest.mark.parametrize(
	argnames=("spec_range", "specs", "expected"),
	argvalues=[
		(SpecifierRange("1.0.0", True, "2.0.0", False), SpecifierSet("==1.0.0"), True),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("==1.0.0"), False),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("==1.0.1"), True),
		(SpecifierRange("1.0.0", True, "2.0.0", False), SpecifierSet("==1.0.1"), True),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("==0.9.9"), False),
		(SpecifierRange("1.0.0", True, "2.0.0", False), SpecifierSet("==0.9.9"), False),
		(SpecifierRange("1.0.0", False, "2.0.0", True), SpecifierSet("==2.0.0"), True),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("==2.0.0"), False),
		(SpecifierRange("1.0.0", False, "2.0.0", True), SpecifierSet("==2.0.1"), False),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("==2.0.0"), False),
		(SpecifierRange("1.0.0", True, "2.0.0", False), SpecifierSet("~=1.0.0"), True),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("~=1.0.0"), False),
		(SpecifierRange("1.0.0", True, "2.0.0", False), SpecifierSet("~=1.0.1"), True),
		(SpecifierRange("1.0.0", False, "2.0.0", False), SpecifierSet("~=1.0.1"), True),
		(SpecifierRange("1.0.0", False, "2.0.1", True), SpecifierSet("~=2.0.0"), False),
		(SpecifierRange("1.0.0", False, "2.1.0", True), SpecifierSet("~=2.0.0"), True),
	],
)
def test_specifier_range_is_subset_for_equals(spec_range: SpecifierRange, specs: SpecifierSet, expected: bool) -> None:
	assert spec_range.is_subset(specs) == expected


@pytest.fixture
def rule_runner() -> RuleRunner:
	return RuleRunner(
		target_types=[DbtProjectTargetGenerator],
		rules=(
			*common_rules(),
			QueryRule(DbtProjectSpec, [DbtProjectTargetGenerator, EnvironmentName]),
			QueryRule(DbtEnvVars, [HydrateDbtEnvVarsRequest]),
		),
	)


@contextmanager
def _wrapped_execution_error():
	try:
		yield
	except ExecutionError as e:
		raise e.wrapped_exceptions[0] if e.wrapped_exceptions else e


@pytest.mark.parametrize(
	argnames=("requires_dbt_version", "packages_file", "packages_contents"),
	argvalues=[
		("<1.7", None, {"packages": [{"a": "b"}, {"c": "d"}]}),
		("<1.7,>=1.5", None, {"packages": [{"a": "b"}, {"c": "d"}]}),
		(["<1.7", ">=1.5"], None, {"packages": [{"a": "b"}, {"c": "d"}]}),
		pytest.param("<1.7,>=1.5", "package-lock.yml", None, marks=pytest.mark.raises(exception=InvalidDbtProject)),
		pytest.param(
			["<1.7", ">=1.5"], "package-lock.yml", None, marks=pytest.mark.raises(exception=InvalidDbtProject)
		),
		(">=1.7", "package-lock.yml", {"packages": [{"a": "b"}, {"c": "d"}]}),
		([">=1.7"], "package-lock.yml", {"packages": [{"a": "b"}, {"c": "d"}]}),
		pytest.param("<1.7", None, {"bad_dict": "yadda"}, marks=pytest.mark.raises(exception=InvalidDbtProject)),
	],
)
def test_load_project_spec_for_target_generator(
	requires_dbt_version: str | list[str],
	packages_file: str | None,
	packages_contents: dict[str, Any],
	rule_runner: RuleRunner,
) -> None:
	project_spec_contents = {"name": "a", "requires-dbt-version": requires_dbt_version}
	package_file_param = f' packages_file="{packages_file}"' if packages_file else ""
	project_files = {
		"a/dbt_project.yml": yaml.safe_dump(project_spec_contents),
		"a/BUILD": f'dbt_project(required_adapters=["dbt-duckdb"],{package_file_param})',
		f"a/{packages_file or 'packages.yml'}": yaml.safe_dump(packages_contents),
	}
	rule_runner.write_files(project_files)
	with _wrapped_execution_error():
		project_spec = rule_runner.request(DbtProjectSpec, [rule_runner.get_target(Address("a"))])
	assert project_spec.project_spec == FrozenDict.deep_freeze(project_spec_contents)
	assert project_spec.digest is not EMPTY_DIGEST
	assert project_spec.packages == FrozenDict.deep_freeze(packages_contents)["packages"]


@pytest.mark.parametrize(
	argnames="request_value",
	argvalues=(
		{"HELLO": "world", "GOODBYE": '"moon"', "HOLA": "'mundo'", "ADIOS": "luna", "CIAO": "bella"},
		".env",
		pytest.param(".fakeenv", marks=pytest.mark.raises(exception=RuntimeError)),
	),
)
def test_hydrate_dbt_env_vars(request_value: dict[str, str] | str, rule_runner: RuleRunner) -> None:
	expected = DbtEnvVars({"HELLO": "world", "GOODBYE": '"moon"', "HOLA": "'mundo'", "ADIOS": "luna", "CIAO": "bella"})
	rule_runner.write_files(
		{"a/.env": "\n".join(["HELLO=world", "GOODBYE='\"moon\"'", "HOLA=\"'mundo'\"", "ADIOS='luna'", 'CIAO="bella"'])}
	)
	with _wrapped_execution_error():
		dbt_env_vars = rule_runner.request(
			DbtEnvVars, [HydrateDbtEnvVarsRequest(RequiredEnvVarsField(request_value, Address("a")))]
		)
	assert dbt_env_vars == expected
