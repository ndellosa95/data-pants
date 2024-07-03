import os

import pytest
from pants.backend.python.macros.python_requirements import PythonRequirementsTargetGenerator
from pants.backend.python.target_types import PythonRequirementTarget
from pants.core.target_types import FileTarget, TargetGeneratorSourcesHelperTarget
from pants.engine.addresses import Address
from pants.engine.fs import PathGlobs, Paths
from pants.engine.target import Dependencies, GeneratedTargets, Target, Targets
from pants.testutil.rule_runner import QueryRule, RuleRunner, run_rule_with_mocks
from pants.util.frozendict import FrozenDict

from ..common_rules import DbtProjectSpec
from ..common_rules import rules as common_rules
from . import DbtConfig, DbtDoc, DbtMacro, DbtModel, DbtProjectTargetGenerator, DbtTest, DbtThirdPartyPackage
from .rules import (
	EXPECTED_EXTENSIONS_MAPPING,
	ConstructTargetsInPathRequest,
	GenerateDbtTargetsRequest,
	construct_targets_in_path,
	generate_dbt_targets,
)
from .rules import rules as target_rules


@pytest.fixture
def rule_runner() -> RuleRunner:
	return RuleRunner(
		target_types=[
			DbtProjectTargetGenerator,
			PythonRequirementTarget,
			FileTarget,
			PythonRequirementsTargetGenerator,
		],
		rules=[
			*target_rules(),
			QueryRule(Targets, [ConstructTargetsInPathRequest]),
			*common_rules(),
			QueryRule(DbtProjectSpec, [DbtProjectTargetGenerator]),
		],
	)


DIR_SPECIFIC_TYPES = frozenset([DbtDoc, DbtConfig]) ^ EXPECTED_EXTENSIONS_MAPPING.keys()


def _matches_path(fn: str, target_type: type[Target], dirpath: str) -> bool:
	return fn.startswith(dirpath) and any(fn.endswith(ext) for ext in EXPECTED_EXTENSIONS_MAPPING[target_type])


@pytest.mark.parametrize(
	argnames=("target_type", "dirpath"),
	argvalues=[(target_type, f"{target_type.alias}s") for target_type in DIR_SPECIFIC_TYPES],
	ids=(target_type.alias for target_type in DIR_SPECIFIC_TYPES),
)
def test_construct_targets_in_path(target_type: type[Target], dirpath: str, rule_runner: RuleRunner) -> None:
	# TODO: test overrides once this issue is closed: https://github.com/pantsbuild/pants/issues/20986
	files = {
		"a/dbt_models/a.sql": "fizz",
		"a/dbt_models/b.sql": "buzz",
		"a/dbt_models/a.yaml": "fizzbuzz",
		"a/dbt_models/b.yml": "foo",
		"a/dbt_models/a.md": "bar",
		"a/dbt_macros/c.sql": "foobar",
		"a/dbt_macros/d.sql": "fizz",
		"a/dbt_macros/d.yaml": "fizzbuzz",
		"a/dbt_macros/c.yml": "foo",
		"a/dbt_macros/d.md": "bar",
		"a/dbt_tests/e.yaml": "fizzbuzz",
		"a/dbt_tests/f.yml": "foo",
		"a/dbt_tests/e.md": "bar",
		"a/dbt_tests/f.sql": "foobar",
		"a/dbt_tests/e.sql": "fizz",
		"a/files/seed.csv": "a,b,c,d,e,f",
		"a/files/seed.md": "yadda",
	}
	rule_runner.write_files(files)
	target_types = (target_type, DbtConfig, DbtDoc)
	request = ConstructTargetsInPathRequest(
		DbtProjectTargetGenerator({"required_adapters": ["dbt-duckdb"]}, Address("a", target_name="project")),
		(dirpath,),
		target_types,
	)
	expected = {
		tt({"source": (relfn := os.path.relpath(fn, "a"))}, request.target_generator.address.create_generated(relfn))
		for tt in target_types
		for fn in files.keys()
		if _matches_path(fn, tt, os.path.join("a", dirpath))
	}
	assert (
		set(
			run_rule_with_mocks(
				construct_targets_in_path,
				rule_args=[request],
				mock_gets=(rule_runner.do_not_use_mock(Paths, [PathGlobs]),),
			)
		)
		== expected
	)


def test_generate_dbt_targets(rule_runner: RuleRunner, sample_project_files: FrozenDict[str, bytes]) -> None:
	rule_runner.write_files(sample_project_files)
	template_address = Address("sample-project", target_name="root")
	target_generator = rule_runner.get_target(template_address)
	hardcoded_fields = {Dependencies.alias: [":db"]}

	def create_file_target(target_type: type[Target], source: str) -> Target:
		source = os.path.relpath(source, template_address.spec_path)
		return target_type({"source": source, **hardcoded_fields}, template_address.create_generated(source))

	def create_sources_helper_target(source: str) -> TargetGeneratorSourcesHelperTarget:
		return TargetGeneratorSourcesHelperTarget({"source": source}, template_address.create_file(source))

	assert run_rule_with_mocks(
		generate_dbt_targets,
		rule_args=[
			GenerateDbtTargetsRequest(
				target_generator,
				template_address,
				{},
				{},
			)
		],
		mock_gets=[
			rule_runner.do_not_use_mock(Targets, [ConstructTargetsInPathRequest]),
			rule_runner.do_not_use_mock(DbtProjectSpec, [DbtProjectTargetGenerator]),
			rule_runner.do_not_use_mock(Paths, [PathGlobs]),
		],
	) == GeneratedTargets(
		target_generator,
		[
			DbtThirdPartyPackage(
				{"specification": {"package": "calogica/dbt_date", "version": "0.10.0"}},
				template_address.create_generated("dbt_date"),
			),
			DbtThirdPartyPackage(
				{"specification": {"package": "dbt-labs/dbt_utils", "version": "1.1.1"}},
				template_address.create_generated("dbt_utils"),
			),
			*(
				create_file_target(DbtMacro, source_file)
				for source_file in sample_project_files.keys()
				if _matches_path(source_file, DbtMacro, os.path.join("sample-project", "macros"))
			),
			*(
				create_file_target(DbtModel, source_file)
				for source_file in sample_project_files.keys()
				if _matches_path(source_file, DbtModel, os.path.join("sample-project", "models"))
			),
			*(
				create_file_target(FileTarget, source_file)
				for source_file in sample_project_files.keys()
				if _matches_path(source_file, FileTarget, os.path.join("sample-project", "seeds"))
			),
			*(
				create_file_target(DbtTest, source_file)
				for source_file in sample_project_files.keys()
				if _matches_path(source_file, DbtTest, os.path.join("sample-project", "data-tests"))
			),
			*(
				create_file_target(DbtConfig, source_file)
				for source_file in sample_project_files.keys()
				if _matches_path(source_file, DbtConfig, os.path.join("sample-project", "models"))
			),
			create_sources_helper_target("dbt_project.yml"),
			create_sources_helper_target("packages.yml"),
			create_sources_helper_target("profiles.yml"),
		],
	)
