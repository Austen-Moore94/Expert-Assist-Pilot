WITH
/*TODO
    1. Sales Currently ASP only need to look into GLOW
*/

DateSelector As (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('2023-01-01' AS DATE), CAST(CURRENT_DATE AS DATE)) )
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

Sales_Flags AS (
    /*Sets the sessions as Eligible/Accepted
      Does this based on if any of the rows in
      the SPD table have the accept or eligible
      Flag equal to true*/
    SELECT 
        spd.customer_session_id as "Session",
        CAST(spd.expert_id AS BIGINT) as "Employee",
        Cast(spd.sales_date_cst AS DATE) as "Date", 
            MAX(
                case when spd.eligibility = 'eligible'
                then 1 else 0 end)
        as "Eligible",
            MAX(
                case when spd.accept_flg = true
                then 1 else 0 end)
        as "Accepted"
    FROM
        hive.care.l3_verizon_asurion_sales_platform_details spd
    WHERE 1=1
        AND spd.channel_name = 'soluto'
        --Show only necessary rows
        AND Cast(spd.sales_date_cst AS DATE)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector) 
                and
                (SELECT MAX(EndDate) FROM DateSelector)
        AND CAST(spd.expert_id AS BIGINT) in (SELECT Employee FROM EligibleEmployees)
    GROUP BY
        spd.customer_session_id,
        spd.expert_id,
        Cast(spd.sales_date_cst AS DATE)
),

Sales AS (
    /*Sums the total eligible and accepted
      Messages per employee per day.*/
    SELECT
        sf."Employee",
        CAST(sf."Date" AS DATE) "Date",
        SUM(sf."Eligible") as "TotalEligible",
        SUM(sf."Accepted") as "TotalAccepted"
    FROM Sales_Flags sf
    GROUP BY
        sf."Employee",
        CAST(sf."Date" AS DATE)
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
        COUNT(eventtype) as "Helix_Searches"
    FROM hive.care.l3_asurion_helix_search_all_events hae
    WHERE
        lower(hae.helix_lob) = 'soluto'
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
)

SELECT
    ee.Employee,
    ee.TenureGroup,
    em.Date,
    em.HoursWorked,
    em.Sessions,
    em.Total_Resolution_Time,
    s.TotalAccepted,
    s.TotalEligible,
    COALESCE(hu.Helix_Searches,0) as "Helix_Searches"
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