{{ config(materialized='table') }}

with source as (

    select *
    from {{ ref('stg_rejected_raw') }}

),

cleaned as (

    select
        -- loan bilgileri
        loan_amount,

        -- tarih
        application_date,

        -- boş title'ları null yap
        nullif(trim(loan_title), '') as loan_title,

        -- 🔴 ANALİZ DIŞI BIRAKMA KARARI
        risk_score,

        -- dti negatif olamaz → veri hatası
        case
            when dti < 0 then null
            else dti
        end as dti,

        zip_code,
        state,

        -- employment_length temizleme
        case
            when employment_length is null then null
            when employment_length like '%10+%' then 10
            when employment_length like '%< 1%' then 0
            else cast(regexp_extract(employment_length, r'\d+') as int64)
        end as employment_length_years,

        case
            when employment_length is null then 'unknown'
            when employment_length like '%10+%' then '10+ years'
            when employment_length like '%< 1%' then '<1 year'
            else employment_length
        end as employment_length_bucket,

        policy_code

    from source

    -- 🔴 Risk score olmayan başvurular analiz dışı
    where risk_score is not null
)

select *
from cleaned