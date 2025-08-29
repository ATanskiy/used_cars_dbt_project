{{ config(
    materialized='incremental',
    unique_key='ad_id'
) }}

with base as (
    select *
    from {{ ref('fact_vehicle_ads') }}
),

joined as (

    select
        {% if is_incremental() %}
            cast(
              (select coalesce(max(id), 0) from {{ this }})
              + row_number() over (order by f.ad_id) as int
            ) as id
        {% else %}
            cast(row_number() over (order by f.ad_id) as int) as id
        {% endif %},

        f.ad_id,
        f.ad_date,

        -- Vehicle
        v.make_normalized as make,
        v.model_normalized as model,
        v.is_make_valid,
        v.is_model_valid,
        v.is_make_model_valid,

        f.year,
        f.has_license,
        f.horse_power,
        f.milage,
        f.s_wheel_side,
        f.complectation,

        v.country_of_origin,
        v.category as vehicle_category,
        v.make_group,
        v.vehicle_type,
        v.body_type,
        v.doors,
        v.seating_capacity,

        -- Engine
        e.engine_type_normalized as engine_type,

        -- Drive type
        d.drive_type_normalized as drive_type,

        -- Transmission
        t.transmission_type_normalized as transmission_type,

        -- Location
        l.normalized_location as location_name,
        l.latitude,
        l.longitude,
        l.country,
        l.country_code,
        l.federalniy_okrug,
        l.oblast,
        l.rayon_okrug,
        l.post_code,
        l.iso3166_2,

        -- Colors
        c.color as vehicle_color,
        c.hex_color,

        -- 
        dd.mentions_dealer,
        dd.mentions_accident_free,
        dd.mentions_original_paint,
        dd.mentions_leather_interior,
        dd.mentions_climate_control,
        dd.mentions_heated_seats,
        dd.mentions_navigation,
        dd.mentions_airbags,
        dd.mentions_immobilizer,
        dd.mentions_trade_in,
        dd.mentions_credit,
        dd.mentions_price_negotiable,
        dd.mentions_exchange,
        dd.mentions_import,

        now() as inserted_at,
        null as updated_at

    from base f

    -- joins
    left join {{ ref('dim_vehicles') }} v
      on f.vehicle_id = v.id

    left join {{ ref('dim_engines') }} e
      on f.engine_id = e.id

    left join {{ ref('dim_drive_types') }} d
      on f.drive_type_id = d.id

    left join {{ ref('dim_transmissions') }} t
      on f.transmission_id = t.id

    left join {{ ref('dim_locations') }} l
      on f.location_id = l.id

    left join {{ ref('dim_vehicle_colors') }} c
      on f.color_id = c.id

    left join {{ ref('dim_descriptions')}} dd
      on f.ad_id = dd.ad_id
)

select * from joined