{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        marka,
        model
    from {{ source('staging', 'staging_daily_ads') }}
),

joined as (
    select
        g.make_model_id,
        g.marka,
        g.model,
        g.make_normalized,
        g.model_normalized,
        g.is_marka_valid as is_make_valid,
        g.is_model_valid as is_model_valid,
        g.is_marka_model_valid as is_make_model_valid,
        g.country_of_origin,
        g.category,
        g.group as make_group,
        g.vehicle_type,
        g.body_type,
        g.doors,
        g.seating_capacity
    from base b
    left join {{ ref('vehicles_cleaned_seed') }} g
      on b.marka = g.marka and b.model = g.model
),

new_records as (
    select *
    from joined

    {% if is_incremental() %}
      except
      select
        make_model_id,
        marka,
        model,
        make_normalized,
        model_normalized,
        is_make_valid,
        is_model_valid,
        is_make_model_valid,
        country_of_origin,
        category,
        make_group,
        vehicle_type,
        body_type,
        doors,
        seating_capacity
        
      from {{ this }}
    {% endif %}
)

select
    {% if is_incremental() %}
        cast((select coalesce(max(id), 0) from {{ this }})
             + row_number() over (order by marka, model) as int) as id
    {% else %}
        cast(row_number() over (order by marka, model) as int) as id
    {% endif %},

    *,
    now() as inserted_at,
    null as updated_at
from new_records