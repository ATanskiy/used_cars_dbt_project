from dagster import asset, define_asset_job, ScheduleDefinition, Definitions
from dagster import asset
from daily_data_loader.run_daily_load import run_daily_load
import subprocess

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

@asset
def dbt_run(dbt_seed):
    """Run dbt run inside Dagster container"""
    subprocess.run(["dbt", "run"], check=True, cwd="/opt/dagster/app/dbt")
    return "dbt run finished ✅"

# ---------- Job + Schedule ----------
# create a job to materialize all assets
materialize_all = define_asset_job(
    name="materialize_all_assets",
    selection="*"
)

# schedule that job every 3 minutes
every_three_minutes = ScheduleDefinition(
    job=materialize_all,
    cron_schedule="*/3 * * * *"
)


# ---------- Definitions ----------
defs = Definitions(
    assets=[daily_run, dbt_seed, dbt_run],
    schedules=[every_three_minutes],
)