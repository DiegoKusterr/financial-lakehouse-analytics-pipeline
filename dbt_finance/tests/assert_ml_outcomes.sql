-- Unknown/gapped outcomes must never become negative training examples.
select features.company_quarter_key
from {{ ref('mart_ml_ready_features') }} as features
left join {{ ref('mart_company_quarterly_metrics') }} as next_quarter
    on features.company_id = next_quarter.company_id
   and next_quarter.fiscal_quarter_start_date = features.fiscal_quarter_start_date + interval '3 months'
where features.next_quarter_revenue_growth_qoq is distinct from next_quarter.revenue_growth_qoq
   or features.next_quarter_growth_flag is distinct from
       case when next_quarter.revenue_growth_qoq is null then null
            when next_quarter.revenue_growth_qoq > 0 then 1 else 0 end
   or features.next_quarter_outcome_available_date is distinct from
       case when next_quarter.revenue_growth_qoq is not null then next_quarter.feature_available_date end
   or features.is_training_eligible is distinct from
       (next_quarter.revenue_growth_qoq is not null
        and next_quarter.feature_available_date > features.feature_available_date)
