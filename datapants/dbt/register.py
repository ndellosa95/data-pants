from .tailor import rules as tailoring_rules
from .target_types import DbtModel, DbtProjectTargetGenerator


def target_types():
	return (DbtProjectTargetGenerator, DbtModel)


def rules():
	return (*tailoring_rules(),)
