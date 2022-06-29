with

final as (


    select
        country_code,
        date,
        safe_cast(replace(exchange_rate, ',', '') as numeric) as exchange_rate,
        safe_cast(replace(euro_super, ',', '') as numeric) as euro_super,
        safe_cast(replace(diesel, ',', '') as numeric) as diesel,
        safe_cast(replace(heating_gasoil, ',', '') as numeric) as heating_gasoil,
        safe_cast(replace(fueloil_sulfur_lt_1, ',', '') as numeric) as fueloil_sulfur_lt_1,
        safe_cast(replace(fueloil_sulfur_ht_1, ',', '') as numeric) as fueloil_sulfur_ht_1,
        safe_cast(replace(lpg, ',', '') as numeric) as lpg

    from {{ source('raw_data', 'raw_prices_2005_with_taxes') }}

)

select * from final
