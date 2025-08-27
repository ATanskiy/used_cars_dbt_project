{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        color, hex_color, r, g, b
    from {{ source('staging', 'staging_daily_ads') }}

),

new_records as (
    select color, hex_color, r, g, b
    from base

    {% if is_incremental() %}
      -- only rows not yet in the target table
      except
      select color, hex_color, r, g, b
      from {{ this }}
    {% endif %}
)

select
    {% if is_incremental() %}
        cast((select max(id) from {{ this }}) 
            + row_number() over (order by color, hex_color, r, g, b) as int) as id
    {% else %}
        cast(row_number() over (order by color, hex_color, r, g, b) as int) as id
    {% endif %},

    color, hex_color, r, g, b,
    now() as inserted_at,
    null as updated_at
from new_records