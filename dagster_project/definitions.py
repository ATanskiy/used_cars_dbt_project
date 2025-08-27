from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from pathlib import Path
from dagster_project.assets import daily_run, dbt_seed, dbt_run, every_three_minutes

from dagster_project import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=[daily_run, dbt_seed, dbt_run],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(Path("/opt/dagster/app/dbt")),
            profiles_dir="/opt/dagster/app", 
        )
    },
    schedules=[every_three_minutes]
)
