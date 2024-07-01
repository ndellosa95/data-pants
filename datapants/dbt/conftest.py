import json
import os
from typing import Any

import pytest
import toml
from pants.backend.python.macros.python_requirements import PythonRequirementsTargetGenerator
from pants.backend.python.target_types import PythonRequirementTarget
from pants.backend.python.util_rules.pex import Pex, PexProcess, PexRequest, PexResolveInfo, VenvPex, VenvPexProcess
from pants.backend.python.util_rules.pex import rules as pex_rules
from pants.backend.python.util_rules.pex_cli import PexPEX
from pants.core.target_types import FileTarget
from pants.engine.environment import EnvironmentName
from pants.engine.fs import Digest, DigestEntries
from pants.engine.process import Process, ProcessResult
from pants.option.global_options import GlobalOptions
from pants.testutil.python_rule_runner import PythonRuleRunner
from pants.testutil.rule_runner import QueryRule
from pants.util.frozendict import FrozenDict

from .common_rules import DbtEnvVars, DbtProjectSpec, HydrateDbtEnvVarsRequest, HydratedDbtProject
from .common_rules import rules as common_rules
from .target_types import DbtProjectTargetGenerator


@pytest.fixture(scope="session")
def sample_project_files() -> FrozenDict[str, bytes]:
	def get_file_bytes(path: str) -> bytes:
		with open(path, "rb") as f:
			return f.read()

	def walk_dir(path: str) -> set[str]:
		return {os.path.join(dp, f) for dp, _, fn in os.walk(path) for f in fn}

	return FrozenDict({p: get_file_bytes(p) for p in (walk_dir("sample-project") | walk_dir("test_lockfiles"))})


@pytest.fixture
def sample_project_rule_runner(sample_project_files: FrozenDict[str, bytes], request) -> PythonRuleRunner:
	rule_runner = PythonRuleRunner(
		target_types=[
			DbtProjectTargetGenerator,
			PythonRequirementTarget,
			FileTarget,
			PythonRequirementsTargetGenerator,
		],
		rules=(
			QueryRule(GlobalOptions, []),
			QueryRule(DigestEntries, (Digest, EnvironmentName)),
			QueryRule(ProcessResult, (Process,)),
			*common_rules(),
			QueryRule(DbtProjectSpec, [DbtProjectTargetGenerator, EnvironmentName]),
			# QueryRule(DbtCli, [DbtProjectTargetGenerator, PythonSetup]),
			QueryRule(DbtEnvVars, [HydrateDbtEnvVarsRequest]),
			# QueryRule(Process, [DbtCliCommandRequest, MkdirBinary, CpBinary, ChmodBinary]),
			QueryRule(HydratedDbtProject, [DbtProjectTargetGenerator, EnvironmentName]),
			*pex_rules(),
			QueryRule(PexPEX, ()),
			QueryRule(Pex, (PexRequest,)),
			QueryRule(VenvPex, (PexRequest,)),
			QueryRule(Process, (PexProcess,)),
			QueryRule(Process, (VenvPexProcess,)),
			QueryRule(ProcessResult, (Process,)),
			QueryRule(PexResolveInfo, (Pex,)),
			QueryRule(PexResolveInfo, (VenvPex,)),
		),
	)
	rule_runner.write_files({"BUILDROOT": b""})

	def fix_options(key: str, opts: dict[str, Any]) -> dict[str, Any]:
		if key == "GLOBAL":
			return {}
		if key == "python":
			interpreter_constraints = [f"==3.{request.param}.*"]
			opts["interpreter_constraints"] = interpreter_constraints
			for k in opts.get("resolves_to_interpreter_constraints", {}):
				opts["resolves_to_interpreter_constraints"][k] = interpreter_constraints
			for k in opts.get("resolves", {}):
				opts["resolves"][k] = os.path.join("test_lockfiles", f"lock{request.param}.txt")
		return opts

	pants_options: dict[str, dict[str, Any]] = toml.loads(
		sample_project_files[os.path.join("sample-project", "pants.toml")].decode()
	)
	rule_runner_env_vars = {
		"_".join(["PANTS", top_level_key, k]).upper(): v if isinstance(v, str) else json.dumps(v)
		for top_level_key, top_level_value in pants_options.items()
		for k, v in fix_options(top_level_key, top_level_value).items()
	}
	rule_runner.set_options((), env=rule_runner_env_vars, env_inherit={"PATH", "PYENV_ROOT", "HOME"})
	rule_runner.write_files(sample_project_files)
	return rule_runner


parametrize_python_versions = pytest.mark.parametrize(
	"sample_project_rule_runner",
	argvalues=range(8, 12),
	ids=(f"CPython 3.{x}" for x in range(8, 12)),
	indirect=True,
)
