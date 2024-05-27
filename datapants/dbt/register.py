from .target_types import DbtModel, DbtProjectTargetGenerator


def target_types():
	return (DbtProjectTargetGenerator, DbtModel)
