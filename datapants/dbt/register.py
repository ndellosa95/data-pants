from .common_rules import rules as common_rules
from .tailor import rules as tailoring_rules
from .target_types import DbtModel, DbtProjectTargetGenerator
from .target_types.rules import rules as target_rules


def target_types():
	return (DbtProjectTargetGenerator, DbtModel)


def rules():
	return (
		*tailoring_rules(),
		*target_rules(),
		*common_rules(),
	)
