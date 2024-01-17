SELECT 
    DATE(
        FROM_ISO8601_TIMESTAMP(
            REPLACE(
                TRIM(element_at(VHE.edp_raw_data_map, 'ExtraData_endRequestTime')),
                ' ','T'
            ))
    ) as "Date",
    element_at(VHE.edp_raw_data_map,'Identities_callSid') AS CallSid,
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map,'ExtraData_messageType') = 'Proactive' THEN element_at(VHE.edp_raw_data_map,'ExtraData_botResponse')  END) As "Proactive Messages",
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map,'ExtraData_messageType') != 'Proactive' THEN element_at(VHE.edp_raw_data_map,'ExtraData_botResponse')  END) As "Reactive Messages",
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map, 'ExtraData_inputMessage')  = 'Wrong issue' THEN 1  END) As "Proactive AutoFeedback - Wrong issue",
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map, 'ExtraData_inputMessage')  = 'Still need help' THEN 1  END) As "Proactive AutoFeedback - Still need help",
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map, 'ExtraData_inputMessage')  = 'That worked!' THEN 1  END) As "Proactive AutoFeedback - That Worked"
FROM 
    hive.care.l1_verizon_home_events VHE
WHERE 1=1
    AND element_at(VHE.edp_raw_data_map, '_header_eventContext_producer') = 'eip-ingestion-data-science'
    AND element_at(VHE.edp_raw_data_map, 'Identities_messageSid') IS NOT NULL
    AND element_at(VHE.edp_raw_data_map, 'Name') = 'RequestSummaryVoice'
    AND element_at(VHE.edp_raw_data_map, 'Scope')='GenerativeAISearchBotVoice'
    AND DATE(edp_updated_date) > DATE('2023-11-01')
    AND TRY(CAST(element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') AS INT)) in (573190, 573276, 572909, 573192, 573585, 573573, 572815, 552121, 576911, 572247)
GROUP BY
    element_at(VHE.edp_raw_data_map,'Identities_callSid'),
    DATE(
        FROM_ISO8601_TIMESTAMP(
            REPLACE(
                TRIM(element_at(VHE.edp_raw_data_map, 'ExtraData_endRequestTime')),
                ' ','T'
            )))