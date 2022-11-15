with cte_epidem_US as (
    select date_month,
           sum(new_tested) as tested,
           sum(new_confirmed) as confirmed,
           sum(new_deceased) as deceased
           --concat(date_part(year, date_record), '-', date_part(month, date_record)) as year_month
    from epidem_US
    group by date_month
    order by to_date(concat(date_month, '-01'))
),
cte_epidem_states as (
    select substring(location_key, 4, 2) as state,
           sum(new_tested) as tested,
           sum(new_confirmed) as confirmed,
           sum(new_deceased) as deceased
    from epidem_states
    group by 1
    order by 1
),
cte_demog_states as (
    select substring(location_key, 4, 2) as state,
           population
    from demog_states
),
cte_vaccine_US_pre as ( --Some entries in vaccinations_US table were found to have negative values.
    select date_month,
           case
                when new_persons_fully_vaccinated < 0 then 0
                else new_persons_fully_vaccinated
           end as fully_vaccinated_persons
    from vaccination_US
),
cte_vaccine_US as (
    select date_month,
           sum(fully_vaccinated_persons) as "FULLY VACCINATED"
    from cte_vaccine_US_pre
    group by 1
    order by to_date(concat(date_month, '-01'))
),
cte_hosp_US as (
    select date_month,
           sum(new_hospitalized_patients) as "HOSPITALIZED PATIENTS",
           sum(new_intensive_care_patients) as "INTENSIVE-CARE PATIENTS",
           sum(new_ventilator_patients) as "VENTILATOR PATIENTS"
    from hospitalized_US
    group 1
    order by to_date(concat(date_month, '-01'))
),
cte_hosp_states as (
    select substring(location_key, 4, 2) as state,
           sum(new_hospitalized_patients) as "HOSPITALIZED PATIENTS",
           sum(new_intensive_care_patients) as "INTENSIVE-CARE PATIENTS",
           sum(new_ventilator_patients) as "VENTILATOR PATIENTS"
    from hospitalized_states
    group by 1
    order by 1
),
cte_epidem_states_CFR_pre as (
    select  date_record,
            location_key as state,
            new_confirmed as confirmed,
            new_deceased as deceased,
            case
                when new_confirmed = 0 then 0
                else (new_deceased/new_confirmed)*100
            end as cfr,
            substring(to_varchar(date_record), 1, 7) as date_month
    from epidem_states_daily
),
cte_govResp_states as (
    select date_record,
           substring(location_key, 4, 2) as state,
           stringency_index,
           date_month
    from govresp_states
)
-- select * from cte_govresp_states;
cte_epidem_states_CFR as (
    select *,
           sum(confirmed) over(partition by state, date_month) as confirmed_month,
           sum(deceased) over(partition by state, date_month) as deceased_month,
           case
                when confirmed_month = 0 then 0
                else (deceased_month/confirmed_month)*100
           end as cfr_month
    from cte_epidem_states_CFR_pre
)
select state,
       date_month,
       sum(confirmed) as monthly_confirmed,
       sum(deceased) as monthly_deceased,
       case
            when sum(confirmed) = 0 then 0
            else sum(deceased)/sum(confirmed)*100
       end as montly_cfr,
       avg(cfr) as original_monthly_cfr_avg,
       sum(cfr) as original_monthly_cfr_sum,
       min(cfr_month) as monthly_cfr_windows --4.25 for AK, March 2020       
from cte_epidem_states_CFR
group by 1, 2
order by 1, to_date(concat(date_month, '-01'));



