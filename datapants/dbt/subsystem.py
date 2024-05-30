from pants.option.option_types import BoolOption
from pants.option.subsystem import Subsystem
from pants.util.strutil import softwrap


class DbtSubsystem(Subsystem):
	options_scope = "dbt"
	help = softwrap(
		"""
        Options for the datapants dbt backend.
                    
        You can find more information on dbt here: https://www.getdbt.com/
    """
	)

	tailor_project_targets = BoolOption(
        default=True,
        help="If true, add `dbt_project` targets with the `tailor` goal.",
        advanced=True,
    )
