import os

import pytest
from pants.engine.addresses import Address
from pants.engine.fs import PathGlobs, Paths
from pants.engine.target import Target
from pants.testutil.rule_runner import RuleRunner, run_rule_with_mocks

from . import DbtConfig, DbtDoc, DbtProjectTargetGenerator
from .rules import EXPECTED_EXTENSIONS_MAPPING, ConstructTargetsInPathRequest, construct_targets_in_path


@pytest.fixture
def rule_runner() -> RuleRunner:
	return RuleRunner()


DIR_SPECIFIC_TYPES = frozenset([DbtDoc, DbtConfig]) ^ EXPECTED_EXTENSIONS_MAPPING.keys()


@pytest.mark.parametrize(
	argnames=("target_type", "dirpath"),
	argvalues=[(target_type, f"{target_type.alias}s") for target_type in DIR_SPECIFIC_TYPES],
	ids=(target_type.alias for target_type in DIR_SPECIFIC_TYPES),
)
def test_construct_targets_in_path(target_type: type[Target], dirpath: str, rule_runner: RuleRunner) -> None:
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
		if fn.startswith(os.path.join("a", dirpath))
		and any(fn.endswith(ext) for ext in EXPECTED_EXTENSIONS_MAPPING[tt])
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


def test_generate_dbt_targets() -> None: ...
