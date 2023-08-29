WITH
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