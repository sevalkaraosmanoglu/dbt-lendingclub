{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('lending_club', 'accepted_raw') }}

),

renamed as (

    select
        id ,
        member_id,
        loan_status,
        issue_d,
        term,
        grade,
        sub_grade,

        loan_amnt,
        funded_amnt,
        funded_amnt_inv,
        installment,

        total_pymnt,
        total_rec_prncp,
        total_rec_int,
        total_rec_late_fee,
        recoveries,
        collection_recovery_fee,
        out_prncp,

        emp_length,
        home_ownership,
        annual_inc,
        verification_status,
        purpose,
        addr_state,
        dti,

        fico_range_low,
        fico_range_high,
        last_fico_range_low,
        last_fico_range_high,

        delinq_2yrs,
        inq_last_6mths,
        mths_since_last_delinq,

        num_tl_30dpd,
        num_tl_120dpd_2m,
        num_tl_90g_dpd_24m,
        num_tl_op_past_12m,
        pct_tl_nvr_dlq,

        revol_bal,
        revol_util,
        tot_hi_cred_lim,
        total_bal_ex_mort,
        total_bc_limit,
        total_il_high_credit_limit,

        pub_rec,
        pub_rec_bankruptcies,
        tax_liens,

        hardship_flag,
        hardship_type,
        hardship_reason,
        hardship_status,
        hardship_amount,
        hardship_dpd,

        debt_settlement_flag,
        settlement_amount,
        settlement_percentage

    from source

)

select *
from renamed