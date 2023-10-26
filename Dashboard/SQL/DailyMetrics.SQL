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

SessionsAndHoursWorked AS(
SELECT
    CAST(sfd.emplid AS BIGINT) as "Employee",
    CAST(sfd.clndr_dt_cst  AS date) "Date",
    SUM(COALESCE(sfd.total_stafftime,0)/3600.00) as "HoursWorked",
    SUM(sfd.total_service_events) as "Sessions",
    SUM(COALESCE(sfd.total_workload,0))as "Total_Resolution_Time"
FROM
    care.l3_asurion_soluto_finance_datamart_v1 sfd
WHERE 1=1
    AND sfd.channel in ('C2C','IVR')
    AND sfd.total_service_events is not null
    AND sfd.total_service_events != 0 
    AND sfd.emplid in (SELECT Employee FROM EligibleEmployees)
    AND CAST(sfd.clndr_dt_cst  AS date)
        BETWEEN 
            (SELECT MAX(StartDate) FROM DateSelector)
            and
            (SELECT MAX(EndDate) FROM DateSelector)
GROUP BY
    CAST(sfd.emplid AS BIGINT),
    CAST(sfd.clndr_dt_cst  AS date)
),

HERO AS(
    SELECT
        CAST(epm.expert_id AS BIGINT) as "Employee",
        epm.date as "Date",
        SUM(COALESCE(epm.denominator,0)) as "hero_solve_score",
        SUM(COALESCE(epm.numerator,0)) as "hero_solve_count"
    FROM hive.care.expert_performance_metrics epm
    WHERE 1=1
        AND epm.metric = 'HERO_Solve'
        AND upper(epm.lob) = 'PSS'
        --Show only necessary rows
        AND epm.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
        AND CAST(epm.date as DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                AND
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        epm.expert_id,
        epm.date
),

HELIX AS(
    SELECT
        CAST(HAE.empid AS BIGINT) AS "Employee",
        CAST(hae.eventdatetime_cst as DATE) AS "Date",
        COUNT(HAE.eventtype) as "Helix Searches",
        COUNT(DISTINCT HAE.helix_sessionid) as "Helix Sessions"
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
        CAST(HAE.empid AS BIGINT),
        CAST(hae.eventdatetime_cst as DATE)
),

Sales AS(
    SELECT
        CAST(epm.expert_id AS BIGINT) as "Employee",
        epm.date as "Date",
        SUM(COALESCE(epm.denominator,0)) as "Total_Eligible",
        SUM(COALESCE(epm.numerator,0)) as "Total_Accepted"
    FROM hive.care.expert_performance_metrics epm
    WHERE 1=1
        AND epm.metric = 'SP100'
        AND upper(epm.lob) = 'PSS'
        --Show only necessary rows
        AND epm.expert_id in (SELECT expert_id_string FROM EligibleEmployees)
        AND CAST(epm.date as DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                AND
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        epm.expert_id,
        epm.date
)

SELECT 
    shw.Employee,
    shw.Date,
    ppx.work_type,
    shw.HoursWorked,
    shw.Sessions,
    shw.Total_Resolution_Time,
    hro.hero_solve_score,
    hro.hero_solve_count,
    hx."Helix Searches",
    hx."Helix Sessions",
    s.Total_Eligible,
    s.Total_Accepted
FROM
    SessionsAndHoursWorked shw
    FULL OUTER JOIN
    HERO hro
        ON  shw.Employee = hro.Employee
        AND shw.Date = hro.Date
    FULL OUTER JOIN
    HELIX hx
        ON  shw.Employee = hx.Employee
        AND shw.Date = hx.Date
    FULL OUTER JOIN
    Sales S
        ON  shw.Employee = s.Employee
        AND shw.Date = s.Date
    FULL OUTER JOIN
    PPX
        ON  shw.Employee = ppx.expert_id
        AND shw.Date = ppx.clndr_date
WHERE 1=1
    AND shw.Sessions is not null