WITH
DateSelector As (
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
        CAST(eh.expert_id AS INTEGER) as "Employee",
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
        , CAST(m.expert_id as INTEGER) as expert_id
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

HELIX AS(
    SELECT
        CAST(HAE.empid AS INTEGER) AS "Employee",
        CAST(hae.eventdatetime_cst as DATE) AS "Date",
        COUNT(HAE.eventtype) as "HelixSearches",
        COUNT(DISTINCT HAE.helix_sessionid) as "HelixSessions"
    FROM
        hive.care.l3_asurion_helix_search_all_events hae
    WHERE 1=1
        AND lower(hae.helix_lob) = 'soluto'
        AND hae.event = 'Search_Helix_Search_Performed'
        --Show only necessary rows
        AND hae.empid in (SELECT expert_id_string FROM EligibleEmployees)
        AND CAST(hae.eventdatetime_cst as DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                AND
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        CAST(HAE.empid AS INTEGER),
        CAST(hae.eventdatetime_cst as DATE)
),

EmployeeMetrics AS(
    SELECT
        CAST(epm.expert_id AS INTEGER) as "Employee",
        CAST(epm.date as DATE) as "Date",
        SUM(CASE WHEN UPPER(epm.metric) = 'SP100' THEN COALESCE(epm.denominator,0) ELSE 0 END) as "Total_Eligible",
        SUM(CASE WHEN UPPER(epm.metric) = 'SP100' THEN COALESCE(epm.numerator,0) ELSE 0 END) as "Total_Accepted",
        SUM(CASE WHEN UPPER(epm.metric) = 'HERO_SOLVE' THEN COALESCE(epm.denominator,0) ELSE 0 END) as "hero_solve_score",
        SUM(CASE WHEN UPPER(epm.metric) = 'HERO_SOLVE' THEN COALESCE(epm.numerator,0) ELSE 0 END) as "hero_solve_count",
        SUM(CASE WHEN UPPER(epm.metric) = 'CRT' THEN COALESCE(epm.denominator,0) ELSE 0 END) as  "Sessions",
        SUM(CASE WHEN UPPER(epm.metric) = 'CRT' THEN COALESCE(epm.numerator,0) ELSE 0 END) as "Total_Resolution_Time"
    FROM hive.care.expert_performance_metrics epm
    WHERE 1=1
        AND UPPER(epm.metric) in ('SP100', 'HERO_SOLVE', 'CRT')
        AND UPPER(epm.lob) in ('PSS', 'SOLUTO')
        --Show only necessary rows
        AND epm.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
        AND CAST(epm.date as DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                AND
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        epm.expert_id,
        CAST(epm.date as DATE)
),

WorkedHours AS(
    SELECT
        CAST(agnt.emplid AS INTEGER) AS "Employee",
        CAST(agnt.event_date_cst AS DATE) AS "Date",
        SUM(COALESCE(agnt.total_time,0.00))/3600.00/*Seconds in an hour*/ AS  "HoursWorked"
    FROM
        hive.care.l3_asurion_twilio_agent_activity_detail agnt
    WHERE 1=1 
        AND UPPER(agnt.worker_state) not in (
            'OFFLINE','UNAVAILABLE','BREAK',
            'LUNCH','MEETING','NOT AVAILABLE')
        AND agnt.emplid in (SELECT expert_id_string FROM EligibleEmployees)
        AND CAST(agnt.event_date_cst AS DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                AND
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        CAST(agnt.emplid AS INTEGER),
        CAST(agnt.event_date_cst AS DATE)
)

SELECT 
    em.Employee,
    em.Date,
    ppx.work_type,
    wh.HoursWorked,
    em.Sessions,
    em.Total_Resolution_Time,
    em.hero_solve_score,
    em.hero_solve_count,
    COALESCE(hx.HelixSearches,0) AS "Helix Searches",
    COALESCE(hx.HelixSessions,0) AS "Helix Sessions",
    em.Total_Eligible,
    em.Total_Accepted
FROM
    EmployeeMetrics em
    LEFT OUTER JOIN
    WorkedHours wh
        on em.Employee = wh.Employee
        AND em.Date = wh.Date
    LEFT OUTER JOIN
    HELIX hx
        ON  em.Employee = hx.Employee
        AND em.Date = hx.Date
    LEFT OUTER JOIN
    PPX
        ON  em.Employee = ppx.expert_id
        AND em.Date = ppx.clndr_date