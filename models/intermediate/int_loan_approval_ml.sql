{{ config(materialized='table') }}

with accepted as (

    select
        loan_amount,
        employment_length,
        dti,
        state,
        1 as loan_approved
    from {{ ref('stg_accepted_raw') }}

),

rejected as (

    select
        loan_amount,
        employment_length,
        dti,
        state,
        0 as loan_approved
    from {{ ref('stg_rejected_raw') }}

)

select * from accepted
union all
select * from rejected