SELECT
    tid.event_date_cst as "Date",
    tid.answering_id   as "Employee",
    tid.segment_id	as callNo,
    tid.TaskQueue,
    SUM(
        COALESCE(tid.interaction_tm_sec, 0.00)
        +COALESCE(tid.wrap_tm_sec, 0.00)
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
    AND tid.TaskQueue in ('VZN_Onboarding_2628','VZN_MTS_Bundle_EN_2233')
    AND tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
    AND rk.business_unit = 'Soluto'
    AND tid.answering_id in ($pilot_experts$)
GROUP BY
    tid.event_date_cst,
    tid.answering_id,
    tid.segment_id,
    tid.TaskQueue