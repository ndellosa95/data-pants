from pants.core.target_types import ResourceSourceField
from pants.engine.rules import collect_rules, rule
from pants.engine.target import GeneratedSources, GenerateSourcesRequest
from pants.engine.unions import UnionRule

from .target_types.base import DbtSourceField

# Comment out until new version
# from pants.backend.sql.target_types import SqlSourceField


# delete once new version
class SqlSourceField(ResourceSourceField):
	pass


class GenerateSqlSourcesFromDbtRequest(GenerateSourcesRequest):
	input = DbtSourceField
	output = SqlSourceField


@rule
async def run_dbt_compile(request: GenerateSqlSourcesFromDbtRequest) -> GeneratedSources: ...


def rules():
	return (*collect_rules(), UnionRule(GenerateSourcesRequest, GenerateSqlSourcesFromDbtRequest))
