{{ config(
    materialized='incremental',
    unique_key='ad_id'
) }}

with base as (
    select
        id                             as ad_id,
        cast(date as date)             as ad_date,
        place                          as original_location,
        cast(cost as numeric)          as price,
        year                           as year,
        cast(has_license as boolean)   as has_license,
        power                          as horse_power,
        probeg                         as milage,
        swheel                         as s_wheel_side,
        complectation,
        currency,
        marka,
        model,
        engine,
        gear,
        transmission,
        r, g, b
    from {{ source('staging', 'staging_daily_ads') }}
),

-- Join to dimensions
joined as (
    select
        b.ad_id,
        b.ad_date,
        -- 🔹 Location
        l.id as location_id,
        -- 🔹 Vehicle make/model
        v.id as vehicle_id,
        -- 🔹 Engine
        e.id as engine_id,
        -- 🔹 Drive type
        d.id as drive_type_id,
        -- 🔹 Transmission
        t.id as transmission_id,
        -- 🔹 Color
        c.id as color_id,

        -- Measures
        b.price,
        b.year,
        b.has_license,
        b.horse_power,
        b.milage,
        case lower(b.s_wheel_side)
                when 'right' then 'R'
                when 'left' then 'L'
                else null
                end as s_wheel_side,
        b.complectation,
        b.currency,

        now() as inserted_at,
        null as updated_at

    from base b

    -- location (normalized by seed)
    left join {{ ref('dim_locations') }} l
      on upper(b.original_location) = upper(l.original_location)

    -- make/model mapping
    left join {{ ref('dim_vehicles') }} v
      on b.marka = v.marka
      and b.model = v.model

    -- engine
    left join {{ ref('dim_engines') }} e
      on lower(b.engine) = lower(e.engine_type_original)

    -- drive type
    left join {{ ref('dim_drive_types') }} d
      on lower(b.gear) = lower(d.drive_type_original)

    -- transmission
    left join {{ ref('dim_transmissions') }} t
      on lower(b.transmission) = lower(t.transmission_type_original)

    -- colors
    left join {{ ref('dim_vehicle_colors') }} c
      on b.r = c.r and b.g = c.g and b.b = c.b
),
final as (
    select
        {% if is_incremental() %}
            cast(
              (select coalesce(max(id), 0) from {{ this }})
              + row_number() over (order by ad_id) as int
            ) as id
        {% else %}
            cast(row_number() over (order by ad_id) as int) as id
        {% endif %},
        *
    from joined
)

select
    *
from final