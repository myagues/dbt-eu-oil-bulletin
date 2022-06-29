{% set numeric_cols = ["euro_super", "diesel", "heating_gasoil", "fueloil_sulfur_lt_1", "fueloil_sulfur_ht_1", "lpg"] %}

with

source as (

    select * from {{ ref('stg_raw_prices_1994') }}

),

currency_euro as (

    select
        date,
        country_code,
        exchange_rate,
        {% for col in numeric_cols %}

            case when price_in_euros
                then {{ col }}_wo_taxes
                else round(safe_cast({{ col }}_wo_taxes / exchange_rate as numeric), 2)
            end as {{ col }}_wo_taxes,

            case when price_in_euros
                then {{ col }}_with_taxes
                else round(safe_cast({{ col }}_with_taxes / exchange_rate as numeric), 2)
            end as {{ col }}_with_taxes,
            {{ col }}_vat,

            case when price_in_euros
                then {{ col }}_excise
                else round(safe_cast({{ col }}_excise / exchange_rate as numeric), 2)
            end as {{ col }}_excise,

        {% endfor %}

    from source

),

final as (

    select
        date,
        country_code,
        round(safe_cast(1 / exchange_rate as numeric), 5) as exchange_rate,
        {% for col in numeric_cols %}

            {{ col }}_wo_taxes,
            case when {{ col }}_with_taxes is null
                then round(safe_cast(({{ col }}_wo_taxes + {{ col }}_excise) * (1 + {{ col }}_vat / 100) as numeric), 2)
                else {{ col }}_with_taxes
            end as {{ col }}_with_taxes,
            {{ col }}_vat,
            {{ col }}_excise,

        {% endfor %}

    from currency_euro

)

select * from final
where extract (year from date) < 2005
