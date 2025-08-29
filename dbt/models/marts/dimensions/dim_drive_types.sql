{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        gear as drive_type
    from {{ source('staging', 'staging_daily_ads') }}

),

new_records as (
    select drive_type as drive_type_original,
        case lower(drive_type)
            when 'forward_control'   then 'Front-Wheel Drive (FWD)'
            when 'all_wheel_drive'   then 'All-Wheel Drive (AWD)'
            when 'rear_drive'        then 'Rear-Wheel Drive (RWD)'
            else null
        end as drive_type_normalized
    from base

    {% if is_incremental() %}
      -- only rows not yet in the target table
      except
      select drive_type_original,
             drive_type_normalized
      from {{ this }}
    {% endif %}
)

select
    {% if is_incremental() %}
        cast((select max(id) from {{ this }}) 
                    + row_number() over (order by drive_type_normalized) as int) as id
    {% else %}
        cast(row_number() over (order by drive_type_normalized) as int) as id
    {% endif %},
    drive_type_original,
    drive_type_normalized,
    now() as inserted_at,
    null as updated_at
from new_records