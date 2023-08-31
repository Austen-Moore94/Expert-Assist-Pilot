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

ml_duplicate_remover AS(

    SELECT
        CAST(ml.trans_id as BIGINT) as trans_id,
        max(ml.created_dt) as last_update_date,
        max(ml.eventid) as last_eventid
    FROM
        hive.care.l1_asurion_app_analytics_eds_ml_pipeline_results ml
    WHERE 1=1
        AND ml.name = 'cxpVoice'
        AND ml.key = 'cxp_prob'
        AND ml.created_dt >= (SELECT MAX(StartDate) FROM DateSelector)
    GROUP BY 
       CAST(ml.trans_id as BIGINT)
),

TransactionSummary as(
    SELECT
        CAST(qi.trans_id as BIGINT) AS trans_id,
        MIN(CAST(qi.expert_id as BIGINT)) as "Employee",
        MIN(CAST(qi.trans_date as date)) as trans_date
    FROM care.l3_quality_insights_transactions qi
    WHERE 1=1 
        AND qi.business_unit = 'pss' 
        AND qi.channel = 'voice'
        AND CAST(qi.trans_date as date) >= (SELECT MAX(StartDate) FROM DateSelector)
        --Show only necessary rows
        AND CAST(qi.expert_id as BIGINT) in (SELECT Employee FROM EligibleEmployees)
        and CAST(qi.trans_date as date)
            BETWEEN 
                (SELECT MAX(StartDate) FROM DateSelector)
                and
                (SELECT MAX(EndDate) FROM DateSelector)
    GROUP BY
        CAST(qi.trans_id as BIGINT)
),

CXP_Scores as(
    SELECT
        ts.trans_date as "Date",
        ts.Employee as "Employee",
        SUM(CAST(ml.value AS DOUBLE)) as CXP_Prob_Sum,
        COUNT(ml.value) as CXP_Prob_Count
    FROM 
        TransactionSummary ts
        LEFT OUTER JOIN
        ml_duplicate_remover dr
            ON ts.trans_id = dr.trans_id
        INNER JOIN
        hive.care.l1_asurion_app_analytics_eds_ml_pipeline_results ml
            ON CAST(ml.trans_id as BIGINT) = dr.trans_id
                AND ml.created_dt = dr.last_update_date
                AND ml.eventid =dr.last_eventid

    WHERE 1=1
        AND ml.name = 'cxpVoice'
        AND ml.key = 'cxp_prob'

    GROUP BY
        ts.trans_date,
        ts.Employee
)

SELECT * FROM CXP_Scores