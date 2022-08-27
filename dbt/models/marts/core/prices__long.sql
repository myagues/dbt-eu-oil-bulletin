{% set numeric_cols = ["euro_super", "diesel", "heating_gasoil", "fueloil_sulfur_lt_1", "fueloil_sulfur_ht_1", "lpg"] %}
{% set product_names = ["'Euro-super 95'", "'Automotive gas oil'", "'Heating gas oil'", "'Fuel oil - Sulphur less than 1%'", "'Fuel oil - Sulphur higher than 1%'", "'LPG - motor fuel'"] %}
{% set product_units = ["'1000L'", "'1000L'", "'1000L'", "'t'", "'t'", "'1000L'"] %}

with

country_lu as (

    select
        country_code,
        country_name,
        currency_code

    from {{ ref('country_codes') }}

),

prices_1994 as (

    {% for col in numeric_cols %}
    select
        date,
        clu.country_name as country_name,
        clu.country_code as country_code,
        {{ product_names[loop.index0] }} as product_name,
        {{ product_units[loop.index0] }} as price_units,
        exchange_rate as euro_exch_rate,
        case when exchange_rate = 1
            then 'EUR'
            else clu.currency_code
        end as currency_code,
        taxes,
        price

    from {{ ref('prices_1994') }} as p1994
    unpivot(
        price
        for taxes
        in ({{ col }}_with_taxes as 1, {{ col }}_wo_taxes as 0)
    )
    left join country_lu as clu using (country_code)
    where price is not null

    {% if not loop.last %}
        union all
    {% endif %}
    {% endfor %}

),

prices_2005 as (

    {% for col in numeric_cols %}
    select
        date,
        clu.country_name as country_name,
        clu.country_code as country_code,
        {{ product_names[loop.index0] }} as product_name,
        {{ product_units[loop.index0] }} as price_units,
        exchange_rate as euro_exch_rate,
        case when exchange_rate = 1
            then 'EUR'
            else clu.currency_code
        end as currency_code,
        taxes,
        price

    from {{ ref('prices_2005') }} as p2005
    unpivot(
        price
        for taxes
        in ({{ col }}_with_taxes as 1, {{ col }}_wo_taxes as 0)
    )
    left join country_lu as clu using (country_code)
    where price is not null

    {% if not loop.last %}
        union all
    {% endif %}
    {% endfor %}

),


final as (

    select
        date,
        country_name,
        country_code,
        product_name,
        price_units,
        currency_code,
        euro_exch_rate,
        price,
        cast(taxes as boolean) as taxes
    from prices_1994

    union distinct

    select
        date,
        country_name,
        country_code,
        product_name,
        price_units,
        currency_code,
        euro_exch_rate,
        price,
        cast(taxes as boolean)as taxes
    from prices_2005

)

select * from final
order by date desc, country_code
