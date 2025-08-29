{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        lower(place) as place
    from {{ source('staging', 'staging_daily_ads') }}
),

joined as (
    select
        g.id,
        g.place as original_location,
        g.places_upd as normalized_location,
        g.latitude as latitude,
        g.longitude as longitude,
        case lower(g.country)
            when 'russia'    then 'Russia'
            when 'россия'    then 'Russia'
            when 'русский'   then 'Russia'
            when 'russian'   then 'Russia'
            when 'kazakhstan' then 'Kazakhstan'
            when 'казахстан' then 'Kazakhstan'
            else initcap(country)
            end as country,
        case lower(g.country_code)
            when 'ru' then 'RU'
            when 'ua' then 'UA'
            when 'am' then 'AM'
            when 'by' then 'BY'
            when 'ch' then 'CH'
            when 'ge' then 'GE'
            when 'kz' then 'KZ'
            when 'lv' then 'LV'
            when 'md' then 'MD'
            when 'th' then 'TH'
            else upper(country_code)   -- normalize anything else to uppercase
        end as country_code,
        initcap(g.federalniy_okrug) as federalniy_okrug,
        initcap(g.oblast) as oblast,
        initcap(g.rayon_okrug) as rayon_okrug,
        g.city_like as city_like,
        g.city_district as city_district,
        g.postcode as post_code,
        g.iso3166_2 as iso3166_2
    from base b
    left join {{ ref('geo_cleaned_seed') }} g
      on b.place = lower(g.place)
)

select *
from joined