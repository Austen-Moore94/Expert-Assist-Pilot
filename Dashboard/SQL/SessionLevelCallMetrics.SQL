WITH
DateSelector As (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('2023-06-05' AS DATE), CAST(CURRENT_DATE AS DATE)) )
        as t ("StartDate","EndDate")
),

EligibleEmployees AS
(
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

payper_session as
(
    SELECT 
        per.email
        , upper(substr(per.email, 1, position('@' IN per.email)-1)) as network_id
        , CAST(per.emplid AS int) emplid
        , per.start_date
        , per.end_date
    FROM
        hive.care.l4_asurion_umt_ppx_expert_roster per
    WHERE
        per.channel = 'Call'
        AND per.emplid in (SELECT Employee FROM EligibleEmployees)
),

ppx AS 
(
    SELECT
        dt.clndr_date
        , CAST(m.expert_id as BIGINT) as expert_id
        , case when ppx.email is not null then 'PPX' else 'Hourly' end as work_type
    FROM
        hive.care.l4_asurion_umt_clndr_date dt
        INNER JOIN hive.care.l3_asurion_whole_home_expert_hierarchy eh ON dt.clndr_date BETWEEN eh.eff_start_dt AND eh.eff_end_dt 
        INNER JOIN hive.care.l3_asurion_whole_home_expert_master m ON eh.expert_id = m.expert_id 
        LEFT JOIN payper_session ppx ON UPPER(split_part(m.win_auth, chr(92),2)) = UPPER(ppx.network_id) and dt.clndr_date between ppx.start_date and ppx.end_date
    WHERE
        dt.clndr_date >= (SELECT MAX(StartDate) FROM DateSelector)
        AND m.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
),

AHT as(
    SELECT
        tid.event_date_cst as "Date",
        tid.answering_id   as "Employee",
        tid.reservation_id as "reservation_id",
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
        AND tid.direction = 'inbound'
        --AND tid.TaskQueue in ('VZN_Onboarding_2628','VZN_MTS_Bundle_EN_2233')
        AND tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
        AND rk.business_unit = 'Soluto'
        AND tid.answering_id in (SELECT expert_id_string FROM EligibleEmployees)
        AND tid.event_date_cst >= (SELECT MAX(StartDate) FROM DateSelector)
    GROUP BY
        tid.event_date_cst,
        tid.answering_id,
        tid.reservation_id,
        tid.TaskQueue
),

Sales as(
    SELECT
        asd.reservation_id AS "reservation_id",
        CAST(MAX(asd.offer_flg) AS INT) AS "Offered",
        CAST(MAX(asd.accept_flg) AS INT) AS "Accepted"
    FROM hive.care.l3_verizon_asurion_sales_platform_details asd
    WHERE 1=1
        AND asd.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
        AND asd.sales_date_cst >= (SELECT MAX(StartDate) FROM DateSelector)
    GROUP BY
        asd.reservation_id
),

FirstForFirstCallResolve as(
    SELECT
        fcr.support_session_id
        SIGN(COALESCE(fcr.fcr_pass,0)) as "ResolvedFirstCall",
        SIGN(COALESCE(fcr.fcr_eligible,0)) as "EligibleForFirstCallResolve"
    FROM

)


SELECT
    CAST(vsc.dw_agent_master_id AS BIGINT) as "Employee",
    CAST(fcr.start_dt AS DATE) as "Date",
    ppx.work_type,
    SUM(COALESCE(fcr.fcr_pass,0)) as "ResolvedFirstCall",
    SUM(COALESCE(fcr.fcr_eligible,0)) as "EligibleForFirstCallResolve"
FROM 
    hive.care.l3_asurion_first_call_resolution fcr 
    LEFT OUTER JOIN
    hive.care.l3_verizon_soluto_crm vsc
        ON fcr.support_session_id = vsc.support_session_id
    LEFT OUTER JOIN
    PPX 
        ON CAST(vsc.dw_agent_master_id AS BIGINT) = ppx.expert_id 
            AND CAST(fcr.start_dt AS DATE) = ppx.clndr_date
WHERE 1=1
    --Show only necessary rows
    AND vsc.dw_agent_master_id in (SELECT expert_id_string FROM EligibleEmployees)
    AND CAST(fcr.start_dt as DATE)
        BETWEEN 
            (SELECT MAX(StartDate) FROM DateSelector)
            AND
            (SELECT MAX(EndDate) FROM DateSelector)
GROUP BY
    CAST(vsc.dw_agent_master_id AS BIGINT),
    CAST(fcr.start_dt as DATE),
    ppx.work_type