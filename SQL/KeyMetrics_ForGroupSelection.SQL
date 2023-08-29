WITH
--TODO ADD in CXP

DateSelector As (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('2023-06-01' AS DATE), CAST(CURRENT_DATE AS DATE)) )
        as t ("StartDate","EndDate")
),

EligibleEmployees AS(
    /*Finds currently active
     Employees in the orlando office and their tenure group */
    SELECT
        CAST(eh.expert_id AS BIGINT) as "Employee",
        eh.tenure_group as "TenureGroup"
    FROM
        hive.care.l3_asurion_whole_home_expert_hierarchy eh
    WHERE 1=1
        AND UPPER(eh.business_unit) = 'PSS'
        AND eh.location = 'flor'
        AND (SELECT MAX(EndDate) FROM DateSelector)
            BETWEEN eh.eff_start_dt and eh.eff_end_dt
),

EfficiencyMetrics AS(
    /*
    Agregates total hous worked and total sessions and finds total resolution time for crt
    */
    SELECT 
        CAST(sfd.emplid AS BIGINT) as "Employee",
        CAST(sfd.clndr_dt_cst  AS date) "Date",
        SUM(COALESCE(sfd.total_stafftime,0)/3600.00) as "HoursWorked",
        SUM(sfd.total_service_events) as "Sessions",
        SUM(COALESCE(sfd.total_workload,0))as "Total_Resolution_Time"
    FROM
        care.l3_asurion_soluto_finance_datamart_v1 sfd
    WHERE 1=1
        AND channel in ('C2C','IVR')
        AND sfd.total_service_events is not null
        AND sfd.total_service_events != 0 
        --Show only necessary rows
        AND CAST(sfd.emplid AS BIGINT) in (SELECT Employee FROM EligibleEmployees)
        and CAST(sfd.clndr_dt_cst  AS date)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                and
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        CAST(sfd.emplid AS BIGINT),
        CAST(sfd.clndr_dt_cst  AS date)
),

HELIX_Usage as  (
    SELECT
        CAST(empid AS BIGINT) AS "Employee",
        CAST(hae.eventdatetime_cst as DATE) AS "Date",
        COUNT(eventtype) as "Helix Searches",
        COUNT(DISTINCT helix_sessionid) as "Sessions With Helix Search"
    FROM hive.care.l3_asurion_helix_search_all_events hae
    WHERE 1=1
        AND lower(hae.helix_lob) = 'soluto'
        AND hae.event = 'Search_Helix_Search_Performed'
        --Show only necessary rows
        AND CAST(hae.empid AS BIGINT) in (SELECT Employee FROM EligibleEmployees)
        and CAST(hae.eventdatetime_cst as DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                and
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        CAST(empid AS BIGINT),
        CAST(hae.eventdatetime_cst as DATE)
),

Sales AS (
    /*Sums the total eligible and accepted
      Messages per employee per day.*/
    SELECT
        CAST(epm.expert_id AS BIGINT) as "Employee",
        epm.date as "Date",
        SUM(COALESCE(epm.denominator,0)) as "Total_Eligible",
        SUM(COALESCE(epm.numerator,0)) as "Total_Accepted"
    FROM hive.care.expert_performance_metrics epm
    WHERE 1=1
        AND epm.metric = 'SP100'
        AND upper(epm.lob) = 'PSS'
        AND TRY_CAST(epm.expert_id AS BIGINT) is not null
        --Show only necessary rows
        AND CAST(epm.expert_id AS BIGINT) in (SELECT Employee FROM EligibleEmployees)
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
    ee.Employee,
    ee.TenureGroup,
    em.Date,
    em.HoursWorked,
    em.Sessions,
    em.Total_Resolution_Time,
    s.Total_Accepted,
    s.Total_Eligible,
    COALESCE(hu."Helix Searches",0) as "Helix_Searches",
    COALESCE(hu."Sessions With Helix Search",0) as "Helix_Sessions"
FROM
    EligibleEmployees ee
    LEFT OUTER JOIN
    EfficiencyMetrics em
        ON ee.Employee = em.Employee
    LEFT OUTER JOIN
    Sales s
        ON ee.Employee = s.Employee
        AND em.Date = s.Date
    LEFT OUTER JOIN
    HELIX_Usage hu
        on ee.Employee = hu.Employee
        AND em.Date = hu.Date
WHERE
    ee.Employee IS NOT NULL