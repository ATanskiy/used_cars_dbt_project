from dagster import asset
import subprocess
from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets
from daily_data_loader.run_daily_load import run_daily_load


DBT_PROJECT_DIR = Path("/opt/dagster/app/dbt")

@asset
def daily_run():
    """Run your ETL daily load process"""
    run_daily_load()
    return "Daily load finished ✅"

@asset
def dbt_seed(daily_run):
    """Run dbt seed inside Dagster container"""
    subprocess.run(["dbt", "seed"], check=True, cwd="/opt/dagster/app/dbt")
    return "dbt seed finished ✅"

@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def dbt_project_assets(context, dbt: DbtCliResource):
    # "build" runs seeds, models, snapshots, and tests
    yield from dbt.cli(["build"], context=context).stream()