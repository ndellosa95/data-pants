from __future__ import annotations

import pytest
from pants.core.goals.tailor import AllOwnedSources, PutativeTarget, PutativeTargets
from pants.testutil.rule_runner import QueryRule, RuleRunner

from .tailor import PutativeDbtProjectsRequest
from .tailor import rules as tailoring_rules
from .target_types.project import DbtProjectTargetGenerator


@pytest.fixture
def rule_runner() -> RuleRunner:
	return RuleRunner(
		target_types=[DbtProjectTargetGenerator],
		rules=[
			*tailoring_rules(),
			QueryRule(PutativeTargets, (PutativeDbtProjectsRequest, AllOwnedSources)),
		],
	)


@pytest.mark.parametrize(
	argnames=("options", "expected"),
	argvalues=[
		(
			[],
			PutativeTargets(
				[
					PutativeTarget.for_target_type(
						DbtProjectTargetGenerator,
						path="b",
						name="project",
						triggering_sources=["b/dbt_project.yml"],
					)
				]
			),
		),
		(["--dbt-tailor-project-targets=False"], PutativeTargets()),
	],
	ids=("Enabled", "Disabled"),
)
def test_tailor(options: list[str], expected: PutativeTargets, rule_runner: RuleRunner):
	rule_runner.write_files(
		{
			"a/dbt_project.yml": "mock content",
			"a/BUILD": f"{DbtProjectTargetGenerator.alias}()",
			"b/dbt_project.yml": "more mock content",
			"c/dbt_project.yml": "other unonwed project file",
		}
	)
	rule_runner.set_options(options)
	assert (
		rule_runner.request(
			PutativeTargets,
			(
				PutativeDbtProjectsRequest(("a", "b")),
				AllOwnedSources(["a/dbt_project.yml"]),
			),
		)
		== expected
	)
