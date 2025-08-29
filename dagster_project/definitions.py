from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from pathlib import Path
from dagster_project import assets, schedules

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(
            project_dir=str(Path("/opt/dagster/app/dbt")),
            profiles_dir="/opt/dagster/app", 
        )
    },
    schedules=[schedules.every_two_minutes]
)
