from dataclasses import dataclass
import logging

from pants.core.goals.tailor import AllOwnedSources, PutativeTarget, PutativeTargets, PutativeTargetsRequest
from pants.engine.fs import PathGlobs, Paths
from pants.engine.rules import Get, collect_rules, rule
from pants.engine.unions import UnionRule

from .subsystem import DbtSubsystem
from .target_types import DbtProjectTargetGenerator


@dataclass(frozen=True)
class PutativeDbtProjectsRequest(PutativeTargetsRequest):
	pass


@rule
async def find_unowned_dbt_projects(
	request: PutativeDbtProjectsRequest, owned_sources: AllOwnedSources, subsystem: DbtSubsystem
) -> PutativeTargets:
	if not subsystem.tailor_project_targets:
		return PutativeTargets()
	all_dbt_project_paths = await Get(Paths, PathGlobs, request.path_globs("dbt_project.yml"))
	return PutativeTargets(
		PutativeTarget(
			path, "project", DbtProjectTargetGenerator.alias, triggering_sources=[path], owned_sources=[path]
		)
		for path in all_dbt_project_paths.files if path not in owned_sources
	)


def rules():
	return (*collect_rules(), UnionRule(PutativeTargetsRequest, PutativeDbtProjectsRequest))
