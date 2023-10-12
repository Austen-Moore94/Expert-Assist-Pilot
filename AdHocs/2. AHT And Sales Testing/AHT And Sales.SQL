WITH 
AHT as(
    SELECT
        tid.event_date_cst as "Date",
        tid.answering_id   as "Employee",
        tid.reservation_id as callNo,
        tid.TaskQueue,
        SUM(
            COALESCE(tid.interaction_tm_sec, 0)
            +COALESCE(tid.wrap_tm_sec, 0)
        )
        as HandleTime
    FROM
        hive.care.l3_asurion_twilio_interaction_detail tid
        LEFT OUTER JOIN
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
            ON lower(tid.interaction_routing_key) = lower(rk.routingrulekey)
            AND tid.event_date_cst between rk.startdate and rk.enddate
    WHERE 1=1
        AND tid.event_date_cst BETWEEN CAST('2023-09-11' as DATE) AND (CURRENT_DATE - interval '1' day)
        AND tid.direction = 'inbound'
        -- AND tid.TaskQueue in ('VZN_Onboarding_2628','VZN_MTS_Bundle_EN_2233')
        AND tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
        AND rk.business_unit = 'Soluto'
        AND tid.answering_id in ($pilot_experts$)
    GROUP BY
        tid.event_date_cst,
        tid.answering_id,
        tid.reservation_id,
        tid.TaskQueue
),

Sales as(
    SELECT
        asd.reservation_id AS callNo,
        CAST(MAX(asd.offer_flg) AS INT) AS "Offered",
        CAST(MAX(asd.accept_flg) AS INT) AS "Accepted"
    FROM hive.care.l3_verizon_asurion_sales_platform_details asd
    WHERE 1=1
        AND asd.sales_date_cst BETWEEN CAST('2023-09-11' as DATE) AND (CURRENT_DATE - interval '1' day)
        AND asd.expert_id in ($pilot_experts$)
    GROUP BY
        asd.reservation_id
)

SELECT
    a.Date,
    a.Employee,
    a.callNo,
    a.TaskQueue,
    a.HandleTime,
    COALESCE(s.Offered,0) as "Offered",
    COALESCE(s.Accepted,0) as "Accepted"
FROM
    AHT a
    LEFT OUTER JOIN
    Sales s
        on a.callNo = s.callNo