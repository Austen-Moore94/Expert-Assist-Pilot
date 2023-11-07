WITH
DateSelector AS (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('$startDate$' AS DATE), CAST('$endDate$' AS DATE), CAST('2023-09-25' AS DATE)) )
        as t ("StartDate","EndDate","ExperAssistLaunch")
),

EligibleEmployees AS (
    /*Finds currently active
     Employees in the orlando office and their tenure group */
    SELECT
        CAST(eh.expert_id AS BIGINT) as "Employee",
        eh.expert_id as expert_id_string,
        eh.tenure_group as "TenureGroup"
    FROM
        hive.care.l3_asurion_whole_home_expert_hierarchy eh
    WHERE 1=1
        AND UPPER(eh.business_unit) = 'PSS'
        AND eh.location = 'flor'
        AND (SELECT MAX(EndDate) FROM DateSelector)
            BETWEEN eh.eff_start_dt and eh.eff_end_dt
        AND eh.expert_id in (
            '293932','321930','364717','393198','426097',
            '536491','538855','547381','547655','548026',
            '548646','549667','550446','552121','552402',
            '554243','554487','561761','567519','567620',
            '568127','568568','568651','569153','569375',
            '569498','570227','572247','572815','572909',
            '573190','573192','573276','573573','573585',
            '574166','575731','575996','576565','576601',
            '576911','577073','577246','578299','579162',
            '579630','580895','580911','581015','581139',
            '581145','581275','581396','581694')
),

AHT AS(
    SELECT
        tid.event_date_cst as "Date",
        CAST(tid.answering_id AS BIGINT) as "Employee",
        tid.reservation_id as callNo,
        tid.TaskQueue,
        SUM(
            COALESCE(tid.interaction_tm_sec, 0)
            +COALESCE(tid.wrap_tm_sec, 0)
        ) as HandleTime,
        MIN(tid.segstart) AS CallStart,
        MAX(tid.segstop) AS CallEnd
        
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

Sales AS(
    SELECT
        asd.reservation_id AS callNo,
        CAST(MAX(asd.offer_flg) AS INT) AS "Offered",
        CAST(MAX(asd.accept_flg) AS INT) AS "Accepted"
    FROM hive.care.l3_verizon_asurion_sales_platform_details asd
    WHERE 1=1
        AND asd.sales_date_cst BETWEEN (SELECT StartDate FROM DateSelector) AND  (SELECT EndDate from DateSelector)
        AND asd.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
    GROUP BY
        asd.reservation_id
),

HELIX AS(
SELECT
    CAST(AT_TIMEZONE(from_iso8601_timestamp(eventDate), 'America/Chicago') AS TIMESTAMP) TimeOfHelixSearch,
    CAST(HAE.empid AS BIGINT) AS Employee,
    COUNT(HAE.eventtype) AS HelixSearches
FROM
    hive.care.l3_asurion_helix_search_all_events hae
WHERE 1=1
    AND lower(hae.helix_lob) = 'soluto'
    AND hae.event = 'Search_Helix_Search_Performed'
    AND hae.empid in (SELECT expert_id_string FROM EligibleEmployees)
    AND CAST(hae.eventdatetime_cst as DATE)
        BETWEEN 
            (SELECT StartDate FROM DateSelector)
            AND
            (SELECT EndDate FROM DateSelector)
GROUP BY
    CAST(AT_TIMEZONE(from_iso8601_timestamp(eventDate), 'America/Chicago') AS TIMESTAMP),
    CAST(HAE.empid AS BIGINT)
),

ExpertAssist AS(
    SELECT
        CAST(element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') AS BIGINT) as Employee,
        cast(substr(element_at(VHE.edp_raw_data_map, 'ExtraData_startRequestTime'), 1, 19) as timestamp) as TimeOfExpertAssistUse,
        COUNT(element_at(VHE.edp_raw_data_map, 'Identities_messageSid')) AS ExpertAssistSearches
    FROM 
        hive.care.l1_verizon_home_events VHE
    WHERE 1=1
        AND element_at(VHE.edp_raw_data_map, '_header_eventContext_producer') = 'eip-ingestion-data-science'
        AND element_at(VHE.edp_raw_data_map, 'Identities_messageSid') IS NOT NULL
        AND element_at(VHE.edp_raw_data_map, 'Name') = 'RequestSummaryVoice'
        AND element_at(VHE.edp_raw_data_map, 'Scope')='GenerativeAISearchBotVoice'
        AND DATE(edp_updated_date) >= (SELECT ExperAssistLaunch FROM DateSelector)
        AND element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') IN (SELECT expert_id_string FROM EligibleEmployees)
    GROUP BY
        CAST(element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') AS BIGINT),
        cast(substr(element_at(VHE.edp_raw_data_map, 'ExtraData_startRequestTime'), 1, 19) as timestamp)

)

SELECT
    a.Date,
    -- a.CallStart,
    -- a.CallEnd,
    CAST(a.Employee as BIGINT) Employee,
    -- a.callNo,
    a.TaskQueue,
    a.HandleTime,
    COALESCE(s.Offered,0) as "Offered",
    COALESCE(s.Accepted,0) as "Accepted",
    COALESCE((SUM(h.HelixSearches)),0) as "HelixSearches",
    COALESCE((SUM(e.ExpertAssistSearches)),0) as "ExpertAssistSearches"
FROM
    AHT a
    LEFT OUTER JOIN
    Sales s
        on a.callNo = s.callNo
    LEFT OUTER JOIN
    HELIX h
        ON a.Employee = h.Employee
        and h.TimeOfHelixSearch BETWEEN a.CallStart AND a.CallEnd
    LEFT OUTER JOIN
    ExpertAssist e
        ON a.Employee = e.Employee
            AND e.TimeOfExpertAssistUse BETWEEN a.CallStart AND a.CallEnd
GROUP BY
    a.Date,
    a.CallStart,
    a.CallEnd,
    a.Employee,
    a.callNo,
    a.TaskQueue,
    a.HandleTime,
    COALESCE(s.Offered,0),
    COALESCE(s.Accepted,0)