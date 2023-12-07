WITH
DateSelector As (
    SELECT *
    FROM
        ( VALUES (CAST('2023-10-02' AS DATE), CAST('2023-12-03' AS DATE)) )
        as t ("StartDate","EndDate")
),


EligibleEmployees AS
(

    SELECT
        e.Employee
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
        SUM(COALESCE(epm.denominator,0)) as "hero_XR_score",
        SUM(COALESCE(epm.numerator,0)) as "hero_XR_count"
    FROM hive.care.expert_performance_metrics epm
    WHERE 1=1
        AND epm.metric = 'HERO XR Auto'
        AND upper(epm.lob) = 'PSS'
        --Show only necessary rows
        AND epm.expert_id in (SELECT CAST(Employee AS VARCHAR) FROM EligibleEmployees)
        AND CAST(epm.date as DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                AND
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        epm.expert_id,
        epm.date
),

Transfers AS(
    SELECT
        CAST(epm.expert_id AS BIGINT) as "Employee",
        epm.date as "Date",
        SUM(COALESCE(epm.denominator,0)) as "Transfer_score",
        SUM(COALESCE(epm.numerator,0)) as "Transfer_count"
    FROM hive.care.expert_performance_metrics epm
    WHERE 1=1
        AND epm.metric = 'Transfers'
        AND upper(epm.lob) = 'PSS'
        --Show only necessary rows
        AND epm.expert_id in (SELECT CAST(Employee AS VARCHAR) FROM EligibleEmployees)
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
        AND hae.empid in (SELECT CAST(Employee AS VARCHAR) FROM EligibleEmployees)
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
        AND epm.expert_id in (SELECT CAST(Employee AS VARCHAR) FROM EligibleEmployees)
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
    e.Employee,
    shw.Date,
    shw.HoursWorked AS Hours_Worked,
    shw.Sessions,
    shw.Total_Resolution_Time,
    hro.HERO_XR_Score,
    hro.HERO_XR_Count,
    trn.Transfer_Score,
    trn.Transfer_Count,
    COALESCE(hx."Helix Searches",0) "Helix_Searches",
    COALESCE(hx."Helix Sessions",0) "Helix_Sessions",
    s.Total_Eligible,
    s.Total_Accepted
FROM
    SessionsAndHoursWorked shw
    FULL OUTER JOIN
    HERO hro
        ON  shw.Employee = hro.Employee
        AND shw.Date = hro.Date
    FULL OUTER JOIN
    Transfers trn
        ON  shw.Employee = trn.Employee
        AND shw.Date = trn.Date
    FULL OUTER JOIN
    HELIX hx
        ON  shw.Employee = hx.Employee
        AND shw.Date = hx.Date
    FULL OUTER JOIN
    Sales S
        ON  shw.Employee = s.Employee
        AND shw.Date = s.Date
    FULL OUTER JOIN 
    EligibleEmployees e
        ON shw.Employee = e.Employee

WHERE 1=1
    AND shw.Sessions is not null