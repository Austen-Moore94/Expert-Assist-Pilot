WITH
DateSelector As (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('2023-09-25' AS DATE), CAST('2023-12-06' AS DATE)) )
        as t ("StartDate","EndDate")
),

EligibleEmployees AS
(
    /*Finds currently active
     Employees in the orlando office and their tenure group */
    SELECT
        CAST(e.Employee AS INT) as "Employee",
        CAST(e.Employee AS VARCHAR(6)) as expert_id_string
    FROM
    (VALUES
        (581139), (569375), (575996), (547655), (548026),
        (548646), (579630), (550446), (569153), (578299),
        (573190), (573276), (572909), (573192), (573585),
        (573573), (572815), (552121), (576911), (572247),
        (536491), (552402), (581396), (567519), (581694),
        (549667), (570227), (568568), (574166), (293932),
        (364717), (426097), (554487), (575731), (576565),
        (577073), (577246), (579162), (580895), (580911),
        (581015), (581145), (581275), (567620), (547381),
        (538855), (561761), (321930), (393198), (568127), 
        (568651), (576601), (569498), (554243)
         ) as e ("Employee")
),

AHT as(
    SELECT
        tid.event_date_cst as "Date",
        tid.answering_id   as "Employee",
        tid.reservation_id,
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
        AND tid.event_date_cst BETWEEN (SELECT StartDate FROM DateSelector) AND  (SELECT EndDate from DateSelector)
        AND tid.direction = 'inbound'
        --AND tid.TaskQueue in ('VZN_Onboarding_2628','VZN_MTS_Bundle_EN_2233')
        AND tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
        AND rk.business_unit = 'Soluto'
        AND tid.answering_id in (SELECT expert_id_string FROM EligibleEmployees)
    GROUP BY
        tid.event_date_cst,
        tid.answering_id,
        tid.reservation_id,
        tid.TaskQueue
),

Sales as(
    SELECT
        asd.reservation_id,
        CAST(MAX(asd.offer_flg) AS INT) AS "Offered",
        CAST(MAX(asd.accept_flg) AS INT) AS "Accepted"
    FROM hive.care.l3_verizon_asurion_sales_platform_details asd
    WHERE 1=1
        AND asd.sales_date_cst BETWEEN (SELECT StartDate FROM DateSelector) AND  (SELECT EndDate from DateSelector)
        AND asd.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
    GROUP BY
        asd.reservation_id
)

-- ,
-- Call_To_Reservation as(
-- SELECT
--     ts.reservationsid,
--     ts.call_sid
-- FROM
--     hive.care.l3_asurion_twilio_task_solution ts
-- WHERE ts.reservation_id in (SELECT tid.reservation_id FROM AHT)
-- GROUP BY
--     reservationsid
-- )

-- ExpertAssistReactiveUse AS (
--     SELECT
--         ctr.reservation_id,
--         COALESCE(COUNT(element_at(VHE.edp_raw_data_map, 'Identities_messageSid')),0) as ExpertAssistMessages
--     FROM 
--         hive.care.l1_verizon_home_events VHE
--         INNER JOIN
--         Call_To_Reservation CTR
--             ON ctr.call_sid = element_at(VHE.edp_raw_data_map, 'Identities_callSid')
--     WHERE 1=1
--         AND element_at(VHE.edp_raw_data_map, '_header_eventContext_producer') = 'eip-ingestion-data-science'
--         AND element_at(VHE.edp_raw_data_map, 'Identities_messageSid') IS NOT NULL
--         AND element_at(VHE.edp_raw_data_map, 'Name') = 'RequestSummaryVoice'
--         AND element_at(VHE.edp_raw_data_map, 'Scope')='GenerativeAISearchBotVoice'
--         AND DATE(edp_updated_date) >= (SELECT StartDate FROM DateSelector)
--         AND element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') IN (SELECT expert_id_string FROM EligibleEmployees)
--     GROUP BY
--         ctr.reservation_id
-- )

SELECT
    a.Date,
    CAST(a.Employee AS INT) AS Employee,
    a.reservation_id,

    a.TaskQueue,
    a.HandleTime as "Handle Time",
    COALESCE(s.Offered,0) as "Offered",
    COALESCE(s.Accepted,0) as "Accepted"
FROM
    AHT a
    LEFT OUTER JOIN
    Sales s
        on a.reservation_id = s.reservation_id
-- GROUP BY
--     a.Date,
--     CAST(a.Employee AS INT)