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

Sales_Flags AS (
    /*Sets the sessions as Eligible/Accepted
      Does this based on if any of the rows in
      the SPD table have the accept or eligible
      Flag equal to true*/
    SELECT
            spd.sales_date_cst
        as "Date",
            CAST(spd.expert_id as BIGINT)
        as "Employee",
            spd.expert_session_id
        as "Session",
            MAX(case when spd.eligibility_flg = true then 1 else 0 end)
        as Eligible,
        --     MAX(case when spd.offer_flg = true then 1 else 0 end)
        -- as Offered,
            MAX(case when spd.accept_flg = true then 1 else 0 end) 
        as Accepted
        --     MAX(case when et.plan_id is null then 0 else 1 end)
        -- as Confirmed_Enrollment,
        --     CAST(MAX(et.thirty_day_churn_num) AS INT)
        -- as Thirty_Day_Churn_Num
    FROM
        hive.care.l3_verizon_asurion_sales_platform_details spd 
        -- LEFT OUTER JOIN
        -- hive.care.l3_verizon_shs_enroll_trans et
        -- on spd.plan_id = et.plan_id
        --     and spd.sales_date_cst <= et.enroll_date
        --     and spd.plan_id <> ''
        --     and spd.plan_id is not null
    WHERE 1=1
        AND spd.channel_name = 'soluto'
         --Show only necessary rows
        AND CAST(spd.expert_id as BIGINT) in (SELECT Employee FROM EligibleEmployees)
        AND spd.sales_date_cst  BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                and
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        spd.sales_date_cst,
        spd.expert_id,
        spd.expert_session_id
),

Sales AS (
    /*Sums the total eligible and accepted
      Messages per employee per day.*/
    SELECT
        sf."Employee",
        Cast(sf."Date" as Date) "Date",
        SUM(sf."Eligible") as "Total_Eligible",
        -- SUM(sf."Offered") as "Total_Offered",
        SUM(sf."Accepted") as "Total_Accepted"
        -- SUM(sf."Confirmed_Enrollment") as "Total_Confirmed",
        -- SUM(sf."Thirty_Day_Churn_Num") as "Total_Thirty_Day_Churn"
    FROM Sales_Flags sf
    GROUP BY
        sf."Employee",
        Cast(sf."Date" as Date) 
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