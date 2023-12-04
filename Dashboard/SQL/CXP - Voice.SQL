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

ml_duplicate_remover AS
(
    SELECT
        CAST(ml.trans_id as BIGINT) as trans_id,
        max(ml.created_dt) as last_update_date,
        max(ml.eventid) as last_eventid
    FROM
        hive.care.l1_asurion_app_analytics_eds_ml_pipeline_results ml
    WHERE 1=1
        AND ml.name = 'cxpVoice'
        AND ml.key = 'cxp_prob'
        AND  ml.created_dt >= format_datetime((SELECT MAX(StartDate) FROM DateSelector),'y-MM-d')
    GROUP BY 
       CAST(ml.trans_id as BIGINT)
),

TransactionSummary AS
(
    SELECT
        CAST(qi.trans_id as BIGINT) AS trans_id,
        MIN(CAST(qi.expert_id as BIGINT)) as "Employee",
        MIN(CAST(qi.trans_date as date)) as trans_date
    FROM care.l3_quality_insights_transactions qi
    WHERE 1=1 
        AND qi.business_unit = 'pss' 
        AND qi.channel = 'voice'
        AND CAST(qi.trans_date as date) >= (SELECT MAX(StartDate) FROM DateSelector)
        --Show only necessary rows
        AND CAST(qi.expert_id  as BIGINT) in (SELECT Employee FROM EligibleEmployees)
        and CAST(qi.trans_date as date)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                and
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        CAST(qi.trans_id as BIGINT)
),

CXP_Scores as(
    SELECT
        ts.trans_date as "Date",
        ts.Employee as "Employee",
        SUM(CAST(ml.value AS DOUBLE)) as CXP_Prob_Sum,
        COUNT(ml.value) as CXP_Prob_Count
    FROM 
        TransactionSummary ts
        LEFT OUTER JOIN
        ml_duplicate_remover dr
            ON ts.trans_id = dr.trans_id
        INNER JOIN
        hive.care.l1_asurion_app_analytics_eds_ml_pipeline_results ml
            ON CAST(ml.trans_id as BIGINT) = dr.trans_id
                AND ml.created_dt = dr.last_update_date
                AND ml.eventid =dr.last_eventid
    WHERE 1=1
        AND ml.name = 'cxpVoice'
        AND ml.key = 'cxp_prob'
    GROUP BY
        ts.trans_date,
        ts.Employee
)


SELECT cxp.*
	, ppx.work_type
FROM CXP_Scores cxp
LEFT JOIN ppx ON cxp.employee = ppx.expert_id and cxp.date = ppx.clndr_date