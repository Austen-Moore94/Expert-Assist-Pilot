WITH
DateSelector As (
    SELECT *
    FROM
        ( VALUES (CAST('$STARTDATE$' AS DATE), CAST('$ENDDATE$' AS DATE)) )
        as t ("StartDate","EndDate")
),

Most_Recent_Expert_Hierarcy AS(
    SELECT
        try(CAST(eh.expert_id as INTEGER)) as expert_id,
        MAX(eh.eff_end_dt) as most_recent
    FROM hive.care.l3_asurion_whole_home_expert_hierarchy eh
    GROUP BY try(CAST(eh.expert_id as INTEGER))
),

Current_Hierarcy AS(
    SELECT
        TRY(CAST(eh.expert_id AS INTEGER)) as "Employee",
        UPPER(eh.site_name) as "Mascot",
        eh.tenure_group as "TenureGroup",
        
        UPPER(jc.jobcode_descr) AS "Job"
    FROM
        hive.care.l3_asurion_whole_home_expert_hierarchy eh
        INNER JOIN
        Most_Recent_Expert_Hierarcy mreh
            on mreh.most_recent = eh.eff_end_dt
            and mreh.expert_id = TRY(CAST(eh.expert_id AS INTEGER)) 
        INNER JOIN hive.care.l2_asurion_hrprd_dbo_asu_jobcodes jc
            ON UPPER(eh.jobcode) = UPPER(jc.jobcode)

    WHERE 1=1
        AND (UPPER(eh.jobcode) IN
            (
            'APC1205', 'APC2216', 'CRN1092', 'CRN1119', 'CRN1125', 'CRN2010',
            'EUR2055', 'PHL5019', 'PHL5075', 'PHL5089', 'PHL5342', 'PHL5343',
            'PSS1003', 'PSS2010'
            ))
),

EligibleEmployees AS(
    SELECT
        MIN(
            DATE(
                FROM_ISO8601_TIMESTAMP(
                    REPLACE(
                        TRIM(element_at(VHE.edp_raw_data_map, 'ExtraData_endRequestTime')),
                        ' ','T'
                    )))
            ) as "Expert_Assist_First_Use_Date",
        element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') AS expert_id_string
    FROM 
        hive.care.l1_verizon_home_events VHE
    WHERE 1=1
        AND element_at(
            VHE.edp_raw_data_map, '_header_eventContext_producer'
            ) = 'eip-ingestion-data-science'
        AND element_at(VHE.edp_raw_data_map, 'Identities_messageSid') IS NOT NULL
        AND element_at(VHE.edp_raw_data_map, 'Name') = 'RequestSummaryVoice'
        AND element_at(VHE.edp_raw_data_map, 'Scope')='GenerativeAISearchBotVoice'
        AND DATE(edp_updated_date) >= (SELECT StartDate FROM DateSelector)
    GROUP BY
        element_at(VHE.edp_raw_data_map, 'ExtraData_agentId')
),

AHT as(
    SELECT
        tid.event_date_cst as "Date",
        CASE WHEN LOWER(tid.direction) = 'outbound' 
            THEN tid.originating_id ELSE tid.answering_id END as "Employee",
        tid.reservation_id as "reservation_id",
        tid.ucid           as "UCID",
        min(tid.segstart)  as "SegStart",
        max(tid.segstop)   as "SegStop",
        MAX(tid.TaskQueue) as "TaskQueue",
        SUM(CASE WHEN LOWER(tid.direction) = 'inbound'
            THEN
                (COALESCE(tid.interaction_tm_sec, 0)
                +COALESCE(tid.wrap_tm_sec, 0))
             ELSE 0 END)
        as HandleTime
    FROM
        hive.care.l3_asurion_twilio_interaction_detail tid
        LEFT OUTER JOIN
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
            ON LOWER(tid.interaction_routing_key) = LOWER(rk.routingrulekey)
            AND tid.event_date_cst between rk.startdate and rk.enddate
    WHERE 1=1
        AND LOWER(tid.direction) in ('inbound', 'outbound')
        AND LOWER(tid.interaction_channel) = 'voice'
        AND (
            LOWER(tid.disposition_action) 
                IN ('handled', 'consult', 'transfer', 'conference')
            OR LOWER(tid.disposition_action) LIKE '%flex%' 
            OR LOWER(tid.disposition_action) = 'abandoned'
            OR (
                LOWER(tid.direction) = 'outbound' 
                AND LOWER(tid.disposition_action) IN ('resv_canceled')
                )
            )
        AND LOWER(rk.business_unit) IN ('soluto', 'pss')
        AND rk.client = 'Verizon'
        AND rk.subclient in (
            'Inbox Collateral','Onboarding','Security Advisor','SMB',
            'SMB - BIS','SMB - Bis Complete','SMB - Security Bundle',
            'SMB - Tech Team','TMP')
        AND tid.event_date_cst 
            BETWEEN (SELECT StartDate FROM DateSelector)
            AND (SELECT EndDate from DateSelector)
    GROUP BY
        tid.event_date_cst,
        CASE WHEN LOWER(tid.direction) = 'outbound'
            THEN tid.originating_id ELSE tid.answering_id END,
        tid.reservation_id,
        tid.ucid
        ,tid.TaskQueue
),

Sales_Details AS (
    SELECT
        glw.sales_offer_date,
        glw.emplid,
        glw.interaction_started_at_cst,
        CASE WHEN LOWER(glw.business_unit) = 'pss' 
            THEN 'soluto' ELSE glw.business_unit END AS "business_unit",
        glw.client_name,
        glw.channel_name,
        glw.univ_call_id,
        glw.glow_offer_session_id AS "sales_session_id",
        glw.product_sku,
        glw.transaction_id,
        glw.transaction_id_type,
        CASE WHEN glw.eligible_flg = 'true' THEN 1 ELSE 0 END AS "eligible",
        CASE WHEN glw.offered_flg  = 'true' THEN 1 ELSE 0 END AS "offered",
        CASE WHEN glw.accepted_flg  = 'true' THEN 1 ELSE 0 END AS "accepted",
        'glow' AS "sales_source"
    FROM hive.care.l3_verizon_sales_offer_details GLW
    WHERE 1=1
        AND LOWER(glw.business_unit) = 'pss'
        AND glw.emplid in (SELECT expert_id_string FROM EligibleEmployees)
        AND DATE(glw.sales_offer_date)
            BETWEEN
                (SELECT StartDate FROM DateSelector)
                AND
                (SELECT EndDate from DateSelector)
    
    UNION ALL
    
    SELECT
        asp.sales_date_cst "sales_offer_date",
        asp.expert_id "emplid",
        asp.sales_dttm_cst "interaction_started_at_cst",
        asp.channel_name "business_unit",
        asp.client_name "client_name",
        asp.sub_channel_name "channel_name",
        asp.asurion_call_id "univ_call_id",
        asp.product_session_id "sales_session_id",
        asp.product_sku,
        asp.reservation_id "transaction_id",
        'reservation_id' "transaction_id_type",
        CASE WHEN asp.eligibility_flg = TRUE THEN 1 ELSE 0 END AS "eligible",
        CASE WHEN asp.offer_flg = TRUE THEN 1 ELSE 0 END AS "offered",
        CASE WHEN asp.accept_flg = TRUE THEN 1 ELSE 0 END AS "accepted",
        'asp' AS "sales_source"
    FROM hive.care.l3_verizon_asurion_sales_platform_details AS ASP
    WHERE 1=1
        AND LOWER(asp.channel_name) in ('soluto','pss')
        AND asp.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
        AND DATE(asp.sales_date_cst)
            BETWEEN
                (SELECT StartDate FROM DateSelector)
                AND
                (SELECT EndDate from DateSelector)
),

Sales_Summary_Res AS(
    SELECT
        sd.transaction_id,
        MIN(sd.interaction_started_at_cst) "interaction_started_at_cst",
        COUNT(sd.sales_session_id) sales_sessions,
        COUNT(DISTINCT sd.univ_call_id) calls,
        MAX(sd.offered) AS "Any_Offer",
        MAX(sd.accepted) AS "Any_Accept"
    FROM
        Sales_Details sd
    WHERE
        (
            transaction_id_type = 'reservation_id'
            AND transaction_id IS NOT NULL
        )
    GROUP BY
        transaction_id
),

Sales_Summary_NonRes AS(
    SELECT
        sd.sales_offer_date,
        sd.emplid,
        sd.business_unit,
        sd.client_name,
        sd.channel_name,
        sd.univ_call_id,
        sd.transaction_id,
        sd.transaction_id_type,
        sd.sales_source,
        MIN(sd.interaction_started_at_cst) "interaction_started_at_cst",
        COUNT(sd.sales_session_id) sales_sessions,
        COUNT(DISTINCT sd.univ_call_id) calls,
        MAX(sd.offered) AS "Any_Offer",
        MAX(sd.accepted) AS "Any_Accept"
    FROM
        Sales_Details sd
    WHERE
        (
            transaction_id_type != 'reservation_id'
            OR transaction_id IS NULL
        )
    GROUP BY
        sales_offer_date,
        emplid,
        business_unit,
        client_name,
        channel_name,
        univ_call_id,
        transaction_id,
        transaction_id_type,
        sales_source
    ORDER BY univ_call_id
)


SELECT
    aht.Date,
    CAST(aht.Employee AS INT) AS "Employee",
    1 AS "Calls",
    -- aht.ucid,
    ch.Mascot,
    DATE_DIFF('day', ee.Expert_Assist_First_Use_Date, aht.Date) 
        AS "Days Since First Expert Assist Use",
    SUM(aht.HandleTime) AS "Handle Time",
    COALESCE(SUM(sr.Any_Offer), 0) AS "SR_Hits",
    COALESCE(SUM(so.Any_Offer), 0) AS "SO_Hits",
    SIGN(SUM(COALESCE(sr.Any_Offer, 0)+ COALESCE(so.Any_Offer, 0))) AS "Any Offer",
    SIGN(SUM(COALESCE(sr.Any_Accept, 0)+ COALESCE(so.Any_Accept, 0))) AS "Any Accept"
FROM
    AHT aht
    LEFT JOIN
    Sales_Summary_Res sr 
        ON aht.reservation_id = sr.transaction_id
    LEFT JOIN
    Sales_Summary_NonRes so
        ON aht.Employee = so.emplid
        AND so.interaction_started_at_cst BETWEEN aht.segstart AND aht.segstop
    INNER JOIN
    EligibleEmployees ee
        ON aht.Employee = ee.expert_id_string
    INNER JOIN
    Current_Hierarcy ch
        on em.Employee = ch.Employee

GROUP BY
    aht.Date,
    aht.Employee,
    aht.ucid,
    DATE_DIFF('day', ee.Expert_Assist_First_Use_Date, aht.Date),
    ch.Mascot