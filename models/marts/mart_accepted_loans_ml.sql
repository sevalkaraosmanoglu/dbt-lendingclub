{{ config(materialized='table') }}

with base as (

    select
        -- identifiers
        loan_id,

        -- target
        is_default,

        -- loan information
        loan_amnt,
        term,
        installment,

        -- borrower information
        annual_inc,
        emp_length,
        home_ownership,
        verification_status,
        dti,

        -- credit score
        fico_range_low,
        fico_range_high,

        -- credit history
        delinq_2yrs,
        inq_last_6mths,
        mths_since_last_delinq,

        num_tl_30dpd,
        num_tl_120dpd_2m,
        num_tl_90g_dpd_24m,
        pct_tl_nvr_dlq,

        -- revolving & balances
        revol_bal,
        revol_util,
        tot_hi_cred_lim,
        total_bal_ex_mort,
        total_bc_limit,
        total_il_high_credit_limit,

        -- public records
        pub_rec,
        pub_rec_bankruptcies,
        tax_liens,

        -- categorical features
        grade,
        sub_grade,
        purpose,
        addr_state as state,

        -- engineered segments
        credit_history_segment,
        credit_limit_segment

    from {{ ref('int_accepted_loan_performance') }}

    -- ML CRITICAL FILTER
    where loan_status in ('Fully Paid', 'Charged Off')

)

select * from base