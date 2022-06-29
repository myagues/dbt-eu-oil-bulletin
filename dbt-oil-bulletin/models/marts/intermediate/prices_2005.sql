with

final as (

    select
        pwt.date as date,
        pwt.country_code as country_code,
        pwt.exchange_rate as exchange_rate,
        pwt.euro_super as euro_super_with_taxes,
        pwt.diesel as diesel_with_taxes,
        pwt.heating_gasoil as heating_gasoil_with_taxes,
        pwt.fueloil_sulfur_lt_1 as fueloil_sulfur_lt_1_with_taxes,
        pwt.fueloil_sulfur_ht_1 as fueloil_sulfur_ht_1_with_taxes,
        pwt.lpg as lpg_with_taxes,
        pwot.euro_super as euro_super_wo_taxes,
        pwot.diesel as diesel_wo_taxes,
        pwot.heating_gasoil as heating_gasoil_wo_taxes,
        pwot.fueloil_sulfur_lt_1 as fueloil_sulfur_lt_1_wo_taxes,
        pwot.fueloil_sulfur_ht_1 as fueloil_sulfur_ht_1_wo_taxes,
        pwot.lpg as lpg_wo_taxes

    from {{ ref('stg_raw_prices_2005_with_taxes') }} as pwt
    inner join {{ ref('stg_raw_prices_2005_wo_taxes') }} as pwot using (date, country_code)

)

select * from final
