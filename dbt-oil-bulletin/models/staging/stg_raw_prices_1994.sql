{% set numeric_cols = ["euro_super", "diesel", "heating_gasoil", "fueloil_sulfur_lt_1", "fueloil_sulfur_ht_1", "lpg"] %}

with

source as (

    select * from {{ source('raw_data', 'raw_prices_1994') }}

),

final as (

    select
        bulletin_number,
        date,
        safe_cast(replace(exchange_rate, ',', '') as numeric) as exchange_rate,
        price_in_euros,
        country_code,
        {% for col in numeric_cols %}
            safe_cast(replace({{col}}_wo_taxes, ',', '') as numeric) as {{col}}_wo_taxes,
            safe_cast(replace({{col}}_with_taxes, ',', '') as numeric) as {{col}}_with_taxes,
            safe_cast(replace({{col}}_vat, ',', '') as numeric) as {{col}}_vat,
            safe_cast(replace({{col}}_excise, ',', '') as numeric) as {{col}}_excise,
        {% endfor %}

    from source

)

select * from final
