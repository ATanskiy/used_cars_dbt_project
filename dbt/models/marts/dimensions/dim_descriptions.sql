{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with base as (
    select distinct
        id as ad_id,
        description,
        row_number() over (partition by id order by id) as rn
    from {{ source('staging', 'staging_daily_descriptions') }}
),

deduped as (
    select
        *
    from base 
    where rn=1
),

enriched as (
    select
        ad_id,
        description,

        -- CONDITION / OWNERSHIP
        case when coalesce(description,'') ~* '(автосалон|дилер|автоцентр)' then 1 else 0 end as mentions_dealer,
        case when coalesce(description,'') ~* '(не бит|без ДТП|чистая история|not crashed)' then 1 else 0 end as mentions_accident_free,
        case when coalesce(description,'') ~* '(оригинал|родная краска)' then 1 else 0 end as mentions_original_paint,
        case when coalesce(description,'') ~* '(кожанный салон|кожа|кожанная обивка)' then 1 else 0 end as mentions_leather_interior,

        -- EQUIPMENT / OPTIONS
        case when coalesce(description,'') ~* '(климат-контроль|кондиционер|air conditioning)' then 1 else 0 end as mentions_climate_control,
        case when coalesce(description,'') ~* '(подогрев|heated\s(seats|steering))' then 1 else 0 end as mentions_heated_seats,
        case when coalesce(description,'') ~* '(навигаци|GPS|Apple CarPlay|Android Auto)' then 1 else 0 end as mentions_navigation,
        case when coalesce(description,'') ~* '(airbag|подушки\sбезопасности)' then 1 else 0 end as mentions_airbags,
        case when coalesce(description,'') ~* '(иммобилайзер|сигнализац)' then 1 else 0 end as mentions_immobilizer,

        -- SALES / FINANCE
        case when coalesce(description,'') ~* '(Trade-?in|трейд-?ин)' then 1 else 0 end as mentions_trade_in,
        case when coalesce(description,'') ~* '(кредит|лизинг|рассрочка|loan)' then 1 else 0 end as mentions_credit,
        case when coalesce(description,'') ~* '(торг|negotiable)' then 1 else 0 end as mentions_price_negotiable,
        case when coalesce(description,'') ~* '(обмен)' then 1 else 0 end as mentions_exchange,

        -- LOCATION / IMPORT
        case when coalesce(description,'') ~* '(растаможен|таможня|без пробега в РФ|Japan|auction)' then 1 else 0 end as mentions_import

    from deduped
),

new_records as (
    select *
    from enriched

    {% if is_incremental() %}
      except
      select
        ad_id,
        description,
        mentions_dealer,
        mentions_accident_free,
        mentions_original_paint,
        mentions_leather_interior,
        mentions_climate_control,
        mentions_heated_seats,
        mentions_navigation,
        mentions_airbags,
        mentions_immobilizer,
        mentions_trade_in,
        mentions_credit,
        mentions_price_negotiable,
        mentions_exchange,
        mentions_import
      from {{ this }}
    {% endif %}
)

select
    {% if is_incremental() %}
        cast((select coalesce(max(id), 0) from {{ this }})
             + row_number() over (order by ad_id) as int) as id
    {% else %}
        cast(row_number() over (order by ad_id) as int) as id
    {% endif %},

    *
from new_records