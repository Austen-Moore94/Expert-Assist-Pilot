SELECT 
    element_at(VHE.edp_raw_data_map,'Identities_callSid') AS CallSid,
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map,'ExtraData_messageType') = 'Proactive' THEN element_at(VHE.edp_raw_data_map,'ExtraData_botResponse')  END) As "Proactive Messages",
    COUNT( CASE WHEN element_at(VHE.edp_raw_data_map,'ExtraData_messageType') != 'Proactive' THEN element_at(VHE.edp_raw_data_map,'ExtraData_botResponse')  END) As "Reactive Messages"
FROM 
    hive.care.l1_verizon_home_events VHE
WHERE 1=1
    AND element_at(VHE.edp_raw_data_map, '_header_eventContext_producer') = 'eip-ingestion-data-science'
    AND element_at(VHE.edp_raw_data_map, 'Identities_messageSid') IS NOT NULL
    AND element_at(VHE.edp_raw_data_map, 'Name') = 'RequestSummaryVoice'
    AND element_at(VHE.edp_raw_data_map, 'Scope')='GenerativeAISearchBotVoice'
    AND DATE(edp_updated_date) > DATE('2023-11-10')
GROUP BY
    element_at(VHE.edp_raw_data_map,'Identities_callSid')