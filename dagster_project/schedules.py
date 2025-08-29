from dagster import define_asset_job, ScheduleDefinition

# Job that materializes ALL assets (chain respected automatically)
materialize_all = define_asset_job(
    name="materialize_all_assets",
    selection="*"
)

# Schedule to run every 3 minutes
every_two_minutes = ScheduleDefinition(
    job=materialize_all,
    cron_schedule="*/2 * * * *"
)