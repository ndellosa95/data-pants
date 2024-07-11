from unittest.mock import Mock

import pytest
import yaml
from pants.engine.addresses import Address, Addresses
from pants.engine.environment import EnvironmentName
from pants.engine.fs import Digest, DigestContents, DigestSubset, FileContent, Snapshot
from pants.engine.target import GeneratedSources, UnexpandedTargets
from pants.testutil.python_rule_runner import PythonRuleRunner
from pants.testutil.rule_runner import MockGet, QueryRule, RuleRunner, run_rule_with_mocks

from .codegen import GenerateSqlSourcesFromDbtModelRequest, WrappedCompiledDbtProjectDigest, generate_model_sources
from .common_rules import DbtProjectSpec
from .common_rules import rules as common_rules
from .conftest import get_file_bytes, parametrize_python_versions, walk_dir
from .target_types import DbtModel, DbtProjectTargetGenerator


@pytest.fixture(scope="module")
def expected_compiled_models() -> dict[str, bytes]:
	return {path: get_file_bytes(path) for path in walk_dir("test_compiled_models")}


@parametrize_python_versions
def test_run_dbt_compile(
	sample_project_rule_runner: PythonRuleRunner, expected_compiled_models: dict[str, bytes]
) -> None:
	wrapped_compiled_dbt_project_digest = sample_project_rule_runner.request(
		WrappedCompiledDbtProjectDigest,
		[sample_project_rule_runner.get_target(Address("sample-project", target_name="root"))],
	)
	assert sample_project_rule_runner.request(
		Snapshot, [wrapped_compiled_dbt_project_digest.digest]
	) == sample_project_rule_runner.make_snapshot(expected_compiled_models)


@pytest.fixture
def rule_runner() -> RuleRunner:
	return RuleRunner(
		target_types=[DbtProjectTargetGenerator],
		rules=(
			*common_rules(),
			QueryRule(UnexpandedTargets, [Addresses, EnvironmentName]),
			QueryRule(DbtProjectSpec, [DbtProjectTargetGenerator]),
		),
	)


def test_generate_model_sources(rule_runner: RuleRunner) -> None:
	project_root = DbtProjectTargetGenerator({"required_adapters": ["dbt-duckdb"]}, Address("a", target_name="project"))
	model_target = DbtModel({"source": "fake_model.sql"}, project_root.address.create_generated("model"))
	rule_runner.write_files(
		{
			"a/BUILD": "dbt_project(name='project', required_adapters=['dbt-duckdb'])",
			"a/dbt_project.yml": yaml.safe_dump({"name": "fake_project"}),
			"a/fake_model.sql": "fake",
		}
	)
	expected_snapshot = rule_runner.make_snapshot({"fake_project/fake_model.sql": "fake compiled"})

	def fake_compiled_project(target: DbtProjectTargetGenerator) -> WrappedCompiledDbtProjectDigest:
		assert target == project_root
		return WrappedCompiledDbtProjectDigest(expected_snapshot.digest)

	result: GeneratedSources = run_rule_with_mocks(
		generate_model_sources,
		rule_args=[GenerateSqlSourcesFromDbtModelRequest(Mock(spec=Snapshot), model_target)],
		mock_gets=[
			rule_runner.do_not_use_mock(UnexpandedTargets, [Addresses]),
			MockGet(WrappedCompiledDbtProjectDigest, (DbtProjectTargetGenerator,), fake_compiled_project),
			rule_runner.do_not_use_mock(DbtProjectSpec, [DbtProjectTargetGenerator]),
			rule_runner.do_not_use_mock(Digest, [DigestSubset]),
			rule_runner.do_not_use_mock(Snapshot, [Digest]),
		],
	)
	assert rule_runner.request(DigestContents, [result.snapshot.digest]) == DigestContents(
		[FileContent("fake_project/fake_model.sql", b"fake compiled")]
	)
