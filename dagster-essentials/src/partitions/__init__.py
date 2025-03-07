import dagster as dg

from ..assets import constants


monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=constants.START_DATE,
    end_date=constants.END_DATE,
)

weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date=constants.START_DATE,
    end_date=constants.END_DATE,
)
