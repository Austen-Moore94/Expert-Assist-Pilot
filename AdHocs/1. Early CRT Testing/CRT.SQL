WITH 
Inbound AS(
    SELECT
        tid.event_date_cst as "Date",
        tid.answering_id   as "Employee",
        1 as "IsInbound",
        tid.segment_id	as callNo,
        tid.TaskQueue,
             COALESCE(tid.interaction_tm_sec,0)
            +COALESCE(tid.wrap_tm_sec,0)
        as CTime
    FROM
        hive.care.l3_asurion_twilio_interaction_detail tid
        LEFT OUTER JOIN
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
            ON lower(tid.interaction_routing_key) = lower(rk.routingrulekey)
            AND tid.event_date_cst between rk.startdate and rk.enddate
    WHERE 1=1
        AND tid.event_date_cst >= CAST('2023-07-03' as DATE)
        AND tid.direction = 'inbound'
        AND tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
        AND rk.business_unit = 'Soluto'
        AND tid.answering_id in ($pilot_experts$)
),


mapping_out AS(
    SELECT
        clndr_date,
        taskqueuename,
        business_unit,
        client,
        subclient,
        type,
        count(routingrulekey) num_of_rrkeys
    FROM
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
        INNER JOIN
        hive.care.l3_asurion_heap_clndr_date cd
            on clndr_date between startdate and enddate
    WHERE clndr_date between CAST('2023-07-03' as DATE) and CURRENT_DATE
    GROUP BY
        clndr_date,
        taskqueuename,
        business_unit,
        client,
        subclient,
        type
),

mapping_out_final as(
    SELECT
        *,
        row_number() OVER (PARTITION BY clndr_date ,taskqueuename ORDER BY num_of_rrkeys desc) as out_only_map
    FROM
        mapping_out
),

Outbound AS(
    SELECT
        tid.event_date_cst as "Date",
        tid.originating_id   as "Employee",
        0 as "IsInbound",
        tid.segment_id as callNo,
        tid.TaskQueue,
             COALESCE(tid.abn_tm_sec,0)
            +COALESCE(tid.ans_tm_sec,0)
            +COALESCE(tid.interaction_tm_sec,0)
            +COALESCE(tid.wrap_tm_sec,0)
        as CTime
    FROM
        hive.care.l3_asurion_twilio_interaction_detail tid
        LEFT OUTER JOIN
        mapping_out_final mof
             ON LOWER(tid.taskqueue) = LOWER(mof.taskqueuename)
             AND mof.out_only_map = 1
             AND mof.clndr_date = tid.event_date_cst
             AND tid.interaction_routing_key IS NULL
    WHERE 1=1
        AND tid.event_date_cst >= CAST('2023-07-03' as DATE)
        AND tid.direction = 'outbound'
        AND tid.disposition_action not in ('resv_time_out','resv_rejected')
        AND mof.business_unit = 'Soluto'
        AND tid.originating_id in ($pilot_experts$)
),

All_Calls AS(
    SELECT * FROM Inbound
    UNION ALL
    SELECT * FROM Outbound
)

SELECT
    ac.Date,
    ac.Employee,
    ac.callNo,
    ac.TaskQueue,
    SUM(CTime) AS  ResolutionTime,
    SUM(CASE WHEN IsInbound = 1 THEN CTime END) HandleTime
FROM
    All_Calls ac
GROUP BY
    ac.Date,
    ac.Employee,
    ac.callNo,
    ac.TaskQueue