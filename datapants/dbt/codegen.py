import os
from dataclasses import dataclass

from pants.core.target_types import ResourceSourceField
from pants.engine.addresses import Addresses
from pants.engine.fs import Digest, DigestSubset, PathGlobs, Snapshot
from pants.engine.process import Process, ProcessResult
from pants.engine.rules import Get, MultiGet, collect_rules, rule
from pants.engine.target import GeneratedSources, GenerateSourcesRequest, UnexpandedTargets
from pants.engine.unions import UnionRule

from .common_rules import DbtCliCommandRequest, DbtProjectSpec, HydratedDbtProject, ensure_generated_address
from .target_types import DbtProjectTargetGenerator
from .target_types.model import DbtModelSourceField
from .target_types.test import DbtTestSourceField

# Comment out until new version
# from pants.backend.sql.target_types import SqlSourceField


# delete once new version
class SqlSourceField(ResourceSourceField):
	pass


class GenerateSqlSourcesFromDbtModelRequest(GenerateSourcesRequest):
	input = DbtModelSourceField
	output = SqlSourceField


class GenerateSqlSourcesFromDbtTestRequest(GenerateSourcesRequest):
	input = DbtTestSourceField
	output = SqlSourceField


@dataclass(frozen=True)
class WrappedCompiledDbtProjectDigest:
	digest: Digest


@rule
async def run_dbt_compile(target: DbtProjectTargetGenerator) -> WrappedCompiledDbtProjectDigest:
	hydrated_project = await Get(HydratedDbtProject, DbtProjectTargetGenerator, target)
	compile_process = await Get(Process, DbtCliCommandRequest, hydrated_project.compile_command())
	compile_results = await Get(ProcessResult, Process, compile_process)
	return WrappedCompiledDbtProjectDigest(compile_results.output_digest)


@rule
async def generate_model_sources(request: GenerateSqlSourcesFromDbtModelRequest) -> GeneratedSources:
	if not ensure_generated_address(request.protocol_target.address, "compile model", warn_only=False):
		raise RuntimeError(f"Cannot compile unowned model at address `{request.protocol_target.address}`")
	dbt_project_targets = await Get(
		UnexpandedTargets, Addresses([request.protocol_target.address.maybe_convert_to_target_generator()])
	)
	dbt_project_target: DbtProjectTargetGenerator = dbt_project_targets.expect_single()
	wrapped_compile_digest, project_spec = await MultiGet(
		Get(WrappedCompiledDbtProjectDigest, DbtProjectTargetGenerator, dbt_project_target),
		Get(DbtProjectSpec, DbtProjectTargetGenerator, dbt_project_target),
	)
	specific_source_digest = await Get(
		Digest,
		DigestSubset(
			wrapped_compile_digest.digest,
			PathGlobs(
				[
					os.path.join(
						project_spec["name"],
						os.path.relpath(
							request.protocol_target[DbtModelSourceField].file_path, dbt_project_target.project_dir
						),
					)
				]
			),
		),
	)
	return GeneratedSources(await Get(Snapshot, Digest, specific_source_digest))


def rules():
	return (*collect_rules(), UnionRule(GenerateSourcesRequest, GenerateSqlSourcesFromDbtModelRequest))
