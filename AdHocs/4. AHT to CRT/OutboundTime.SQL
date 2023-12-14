WITH
DateSelector As (
    /*
    Builds a table with min and max dates.
    Kind of overengineered but it prevents dates
    getting changed in one place but not another.
    */
    SELECT *
    FROM
        ( VALUES (CAST('$DATE$' AS DATE), (DATE '2023-11-10') ) )
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
)


SELECT
    tid.event_date_cst   as "Date",
    tid.originating_id   as "Employee",
    tid.reservation_id,
    tid.TaskQueue,
    SUM(
        COALESCE(tid.ans_tm_sec, 0)
        +COALESCE(tid.abn_tm_sec, 0)
        +COALESCE(tid.interaction_tm_sec, 0)
        +COALESCE(tid.wrap_tm_sec, 0)
        
    )
    as ResolveTime
FROM
    hive.care.l3_asurion_twilio_interaction_detail tid
    LEFT OUTER JOIN
    hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
        ON lower(tid.interaction_routing_key) = lower(rk.routingrulekey)
        AND tid.event_date_cst between rk.startdate and rk.enddate
WHERE 1=1
    AND tid.event_date_cst BETWEEN (SELECT StartDate FROM DateSelector) AND  (SELECT EndDate from DateSelector)
    AND tid.direction = 'outbound'
    AND tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
    --AND rk.business_unit = 'Soluto'
    AND tid.originating_id in (SELECT expert_id_string FROM EligibleEmployees)
GROUP BY
    tid.event_date_cst,
    tid.originating_id,
    tid.reservation_id,
    tid.TaskQueue
