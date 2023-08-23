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
    -- AND  CAST(hae.empid AS BIGINT) = 589355
    and CAST(hae.eventdatetime_cst as DATE)
        BETWEEN 
            (SELECT MAX(StartDate) FROM DateSelector)
            and
            (SELECT MAX(EndDate) FROM DateSelector)
GROUP BY
    CAST(empid AS BIGINT),
    CAST(hae.eventdatetime_cst as DATE)