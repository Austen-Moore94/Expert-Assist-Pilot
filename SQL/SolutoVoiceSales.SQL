SELECT
        spd.sales_date_cst
    as "Date",
        spd.expert_id
    as "Employee",
        spd.expert_session_id
    as "Session",
        MAX(case when spd.eligibility_flg = true then 1 else 0 end)
    as Eligible,
        MAX(case when spd.offer_flg = true then 1 else 0 end)
    as Offered,
        MAX(case when spd.accept_flg = true then 1 else 0 end) 
    as Accepted,
        MAX(case when et.plan_id is null then 0 else 1 end)
    as Confirmed_Enrollment,
        CAST(MAX(et.thirty_day_churn_num) AS INT)
    as Thirty_Day_Churn_Num
FROM
    hive.care.l3_verizon_asurion_sales_platform_details spd 
    LEFT OUTER JOIN
    hive.care.l3_verizon_shs_enroll_trans et
    on spd.plan_id = et.plan_id
        and spd.sales_date_cst <= et.enroll_date
        and spd.plan_id <> ''
        and spd.plan_id is not null
WHERE
    (spd.sales_date_cst between date('2023-04-01') and date('2023-06-14')
    or spd.sales_date_cst between date('2023-06-29') and date_add('day',-1,current_date))
    and spd.channel_name = 'soluto'
GROUP BY
    spd.sales_date_cst,
    spd.expert_id,
    spd.expert_session_id