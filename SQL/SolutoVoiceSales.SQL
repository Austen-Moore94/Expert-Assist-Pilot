WITH

DateSelector As (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('2023-03-01' AS DATE), CAST(CURRENT_DATE AS DATE)) )
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
)

SELECT
    epm.expert_id as "Employee",
    epm.date as "Date",
    SUM(COALESCE(epm.numerator,0)) as "Offered",
    SUM(COALESCE(epm.denominator,0)) as "Accepted"
FROM hive.care.expert_performance_metrics epm
WHERE 1=1
    AND epm.metric = 'SP100'
    -- AND emp.lob = 'PSS'
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