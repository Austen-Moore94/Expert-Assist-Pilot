-- WITH
-- ml_duplicate_remover AS(

--     SELECT
--         ml.trans_id,
--         max(ml.created_dt) as last_update_date
--     FROM
--         hive.care.l1_asurion_app_analytics_eds_ml_pipeline_results ml
--     WHERE 1=1
--         AND ml.name = 'cxpVoice'
--         AND ml.key = 'cxp_prob'
--         AND ml.created_dt >= '2023-06-01'
--     GROUP BY 
--         ml.trans_id
-- )

SELECT
    *
FROM
    hive.care.l1_asurion_app_analytics_eds_ml_pipeline_results ml
    -- INNER JOIN
    -- ml_duplicate_remover dr
    --     ON ml.trans_id = dr.trans_id
    --         AND ml.created_dt = dr.last_update_date
WHERE 1=1
        AND ml.name = 'cxpVoice'
        AND ml.key = 'cxp_prob'
    --AND cast(cht.chat_start_dt_cst as date) >= cast('2023-03-01' as date)
-- GROUP BY
--     cht.closed_expert_id,
--     cht.chat_start_dt_cst
LIMIT 10