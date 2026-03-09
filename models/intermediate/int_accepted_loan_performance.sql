-- models/intermediate/int_accepted_loan_performance.sql

with source as (

    select
        id as loan_id,
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

    from {{ ref('stg_accepted_performance') }}

),

final as (

    select
        *,

        -- status flags
        case when loan_status = 'Charged Off' then 1 else 0 end as is_default,
        case when loan_status = 'Fully Paid' then 1 else 0 end as is_fully_paid,
        case when loan_status = 'Current' then 1 else 0 end as is_current,

        -- loss
        case
          when loan_status = 'Charged Off'
            then loan_amnt - coalesce(total_rec_prncp,0) - coalesce(recoveries,0)
          else 0
        end as loss_amount,

        safe_divide(total_rec_prncp, loan_amnt) as repayment_ratio,

        -- risk flags
        case when num_tl_30dpd > 0 then 1 else 0 end as has_any_dpd,
        case when num_tl_120dpd_2m > 0 then 1 else 0 end as has_severe_dpd,
        case when pct_tl_nvr_dlq = 100 then 1 else 0 end as never_delinquent,

        -- segments
        case
          when num_tl_120dpd_2m > 0 then 'severe_dpd'
          when num_tl_30dpd > 0 then 'minor_dpd'
          else 'clean'
        end as credit_history_segment,

        case
          when tot_hi_cred_lim < 20000 then 'low_limit'
          when tot_hi_cred_lim between 20000 and 60000 then 'mid_limit'
          else 'high_limit'
        end as credit_limit_segment,

        case
          when hardship_flag = true then 'hardship'
          else 'no_hardship'
        end as hardship_segment

    from source
)

select * from final