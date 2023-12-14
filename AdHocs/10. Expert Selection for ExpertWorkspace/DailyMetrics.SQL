WITH
DateSelector As (
    SELECT *
    FROM
        ( VALUES (CAST('2023-08-31' AS DATE), CAST('2023-12-03' AS DATE)) )
        as t ("StartDate","EndDate")
),


EligibleEmployees AS
(

    SELECT
        e.Employee
    FROM
        (
        VALUES
            --Random Group
            (575996), (569375), (581015), (547655), (572222), (572269), (567799), (546357), (578273), (546831),
            (547381), (578299), (580867), (550910), (548646), (550446), (574176), (568545), (569153), (579630),
            (552704),
            --PreSelected Test
            (548026), (426097),
            --PreSelected Control
            (597572), (596097), (597580), (597576), (597574), (597685), (597624)
        ) as e ("Employee")
),


SessionsAndHoursWorked AS(
SELECT
    CAST(sfd.emplid AS INT) as "Employee",
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
    CAST(sfd.emplid AS INT),
    CAST(sfd.clndr_dt_cst  AS date)
),

HERO AS(
    SELECT
        CAST(epm.expert_id AS INT) as "Employee",
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
        CAST(epm.expert_id AS INT) as "Employee",
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
        CAST(HAE.empid AS INT) AS "Employee",
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
        CAST(HAE.empid AS INT),
        CAST(hae.eventdatetime_cst as DATE)
),

Sales AS(
    SELECT
        CAST(epm.expert_id AS INT) as "Employee",
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
    COALESCE(trn.Transfer_Score,0) Transfer_Score,
    COALESCE(trn.Transfer_Count,0) Transfer_Count, 
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
ORDER BY
    shw.Date,
    shw.Employee