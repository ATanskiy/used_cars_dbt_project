{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        engine as engine
    from {{ source('staging', 'staging_daily_ads') }}

),

new_records as (
    select  engine as engine_original,
        case lower(engine)
            when 'gasoline' then 'Petrol/Gasoline'
            when 'diesel'   then 'Diesel'
            when 'electro'  then 'Electric'
            when 'hybrid'   then 'Hybrid'
            when 'lpg'      then 'LPG (Liquefied Petroleum Gas)'
            else null
        end as engine_type_normalized
    from base

    {% if is_incremental() %}
      -- only rows not yet in the target table
      except
      select engine_original,
             engine_type_normalized
      from {{ this }}
    {% endif %}
)

select
    {% if is_incremental() %}
        cast((select max(id) from {{ this }}) 
                    + row_number() over (order by engine_type_normalized) as int) as id
    {% else %}
        cast(row_number() over (order by engine_type_normalized) as int) as id
    {% endif %},

    engine_original,
    engine_type_normalized,
    now() as inserted_at,
    null as updated_at
from new_records