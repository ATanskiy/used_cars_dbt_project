{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        transmission 
    from {{ source('staging', 'staging_daily_ads') }}

),

new_records as (
    select transmission as transmission_type_original,
        case lower(transmission)
            when 'mechanical' then 'Manual (MT)'
            when 'automatic'  then 'Automatic (AT)'
            when 'variator'   then 'CVT'
            when 'robot'      then 'AMT/DCT'
            else null
        end as transmission_type_normalized
    from base

    {% if is_incremental() %}
      -- only rows not yet in the target table
      except
      select transmission_type_original,
             transmission_type_normalized
      from {{ this }}
    {% endif %}
)

select
    {% if is_incremental() %}
        cast((select max(id) from {{ this }}) 
                    + row_number() over (order by transmission_type_normalized) as int) as id
    {% else %}
        cast(row_number() over (order by transmission_type_normalized) as int) as id
    {% endif %},

    transmission_type_original,
    transmission_type_normalized,
    now() as inserted_at,
    null as updated_at
from new_records