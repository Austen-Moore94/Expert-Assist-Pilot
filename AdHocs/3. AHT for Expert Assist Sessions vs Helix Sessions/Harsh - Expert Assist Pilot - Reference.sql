with sales as (
	select 
		'glow' as sales_source, sales_offer_date, emplid, univ_call_id, null as reservation_id, eligible_flg, accepted_flg
	from 
		l3_verizon.sales_offer_details
	where 
		sales_offer_date >= CAST('2023-08-28' as DATE)
		and emplid in 
			(
			'293932','321930','364717','393198','426097', '536491','538855','547381','547655','548026',
			'548646','549667','550446','552121','552402', '554243','554487','561761','567519','567620',
			'568127','568568','568651','569153','569375', '569498','570227','572247','572815','572909', 
			'573190','573192','573276','573573','573585', '574166','575731','575996','576565','576601', 
			'576911','577073','577246','578299','579162', '579630','580895','580911','581015','581139', 
			'581145','581275','581396','581694'
			)
		and sales_offer_date < current_date
union all
	select 
		'asp' as sales_source, sales_date_cst, expert_id as emplid, asurion_call_id as univ_call_id, 
		reservation_id as reservation_id, 
		case when sum(case when eligibility_flg = true then 1 else 0 end) > 0 then 'true' else 'false' end as eligible_flg, 
		case when sum(case when accept_flg = true then 1 else 0 end) > 0 then 'true' else 'false' end as accepted_flg
	from 
		sales_analytics.l3_verizon_asurion_sales_platform_details
	where 
		sales_date_cst >= CAST('2023-08-28' as DATE)
		and sales_date_cst < current_date
		and product_sku = '1614'
		and expert_id in
			(
			'293932','321930','364717','393198','426097', '536491','538855','547381','547655','548026',
			'548646','549667','550446','552121','552402', '554243','554487','561761','567519','567620',
			'568127','568568','568651','569153','569375', '569498','570227','572247','572815','572909', 
			'573190','573192','573276','573573','573585', '574166','575731','575996','576565','576601', 
			'576911','577073','577246','578299','579162', '579630','580895','580911','581015','581139', 
			'581145','581275','581396','581694'
			)
		group by 
			sales_date_cst, expert_id, asurion_call_id, reservation_id
), transfers as (
	select 
		cd.univ_call_id as transfer_id, cd1.ans_login_id as from_agentid
	from 
		l3_asurion.call_xfer_detail xd
	left join 
		l3_asurion.call_detail cd
		on xd.dw_src_call_id = cd.dw_call_id
		and date(cd.seg_start_dt) >= date('2023-08-28')
		and xd.date >= date('2023-08-28')
	left join 
		l3_asurion.call_detail cd1
		on xd.dw_call_id = cd1.dw_call_id
		and date(cd1.seg_start_dt) >= date('2023-08-28')
		and xd.date >= date('2023-08-28')
	where 
		xd.date >= date('2023-08-28')
		and cd.univ_call_id is not null
		and cd1.ans_login_id is not null
), expert_names as (
	select 
		expert_id, expert_full_name, 
		case when term_dt > current_date then 1 else 0 end as active_flg,
		case when date_diff('day', min(coalesce(rehire_dt, hire_dt)), current_date) <= 360 
			 then cast(ceiling(date_diff('day', min(coalesce(rehire_dt, hire_dt)), current_date)/30.0000)*30 as varchar)
			 else '360+'
		end as tenure_group
	from 
		l3_asurion.whole_home_expert_master
	where expert_id in 
			(
			'293932','321930','364717','393198','426097', '536491','538855','547381','547655','548026',
			'548646','549667','550446','552121','552402', '554243','554487','561761','567519','567620',
			'568127','568568','568651','569153','569375', '569498','570227','572247','572815','572909', 
			'573190','573192','573276','573573','573585', '574166','575731','575996','576565','576601', 
			'576911','577073','577246','578299','579162', '579630','580895','580911','581015','581139', 
			'581145','581275','581396','581694'
			)
	group by 
		expert_id, expert_full_name, 
		case when term_dt > current_date then 1 else 0 end
), calltimes_initial as (
	select 
		event_date_cst, interaction_routing_key, taskqueue, direction, ucid, reservation_id, disposition_action, originating_id, answering_id, 
		case when answering_id in ('364717', '426097', '547655', '552121', '554487', '569375', '572247', '572815', '572909', '573190', '573192', '573276', '573573', '573585', '575731', '575996', '576565', '576911', '579162', '580895', '580911', '581015', '581139', '581275'
					   ) then 'pilot'
			else 'control'
		end as agent_type, rk.business_unit,
		sl_acceptable, abn_tm_sec, ans_tm_sec, interaction_tm_sec, wrap_tm_sec, hold_tm_sec
	from l3_asurion.twilio_interaction_detail tid
        left join
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
        on lower(tid.interaction_routing_key) = lower(rk.routingrulekey)
        and tid.event_date_cst between rk.startdate and rk.enddate
	where 1=1
	and tid.event_date_cst >= CAST('2023-08-28' as DATE)
	and tid.event_date_cst < current_date
	--and tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
	and tid.answering_id in 
			(
			'293932','321930','364717','393198','426097', '536491','538855','547381','547655','548026',
			'548646','549667','550446','552121','552402', '554243','554487','561761','567519','567620',
			'568127','568568','568651','569153','569375', '569498','570227','572247','572815','572909', 
			'573190','573192','573276','573573','573585', '574166','575731','575996','576565','576601', 
			'576911','577073','577246','578299','579162', '579630','580895','580911','581015','581139', 
			'581145','581275','581396','581694'
			)
		and direction = 'inbound'
union all 
	select 
		event_date_cst, interaction_routing_key, taskqueue, direction, ucid, reservation_id, disposition_action, originating_id, answering_id, 
		case when originating_id in ('364717', '426097', '547655', '552121', '554487', '569375', '572247', '572815', '572909', '573190', '573192', '573276', '573573', '573585', '575731', '575996', '576565', '576911', '579162', '580895', '580911', '581015', '581139', '581275'
					   ) then 'pilot'
			else 'control'
		end as agent_type, rk.business_unit,
		sl_acceptable, abn_tm_sec, ans_tm_sec, interaction_tm_sec, wrap_tm_sec, hold_tm_sec
	from l3_asurion.twilio_interaction_detail tid
        left join
        hive.care.l4_asurion_umt_routing_key_analytics_mapper rk
        on lower(tid.interaction_routing_key) = lower(rk.routingrulekey)
        and tid.event_date_cst between rk.startdate and rk.enddate
	where 1=1
	and tid.event_date_cst >= CAST('2023-08-28' as DATE)
	and tid.event_date_cst < current_date
	--and tid.disposition_action in ('handled','consult','conference','transfer','flex_int_transfer_WARM','flex_int_transfer_COLD')
	and tid.originating_id in 
			(
			'293932','321930','364717','393198','426097', '536491','538855','547381','547655','548026',
			'548646','549667','550446','552121','552402', '554243','554487','561761','567519','567620',
			'568127','568568','568651','569153','569375', '569498','570227','572247','572815','572909', 
			'573190','573192','573276','573573','573585', '574166','575731','575996','576565','576601', 
			'576911','577073','577246','578299','579162', '579630','580895','580911','581015','581139', 
			'581145','581275','581396','581694'
			)
	and tid.taskqueue != 'Dummy Queue for Transfer Tracking'
		and direction = 'outbound'
), calltimes as (
	select 
		*, 
		case when direction = 'inbound' then answering_id else originating_id end as emplid, 
		row_number() over(order by ucid, reservation_id, event_date_cst) as row_n
	from 
		calltimes_initial		
), callids as (
	select 
		cast(event_date_cst as date) as event_date, 
		json_extract_scalar(taskattributes, '$.conversations.conversation_attribute_3') as task_univ_call_id,
		json_extract_scalar(taskattributes, '$.conversations.conversation_attribute_6') as task_emplid,
		min(event_timestamp_cst) as start_time, 
		max(event_timestamp_cst) as end_time
	from 
		sales_analytics.l3_asurion_twilio_task
	where 
		cast(event_date_cst as date) >= date('2023-08-28')
		and json_extract_scalar(taskattributes, '$.conversations.conversation_attribute_6') in (
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
	     '581145','581275','581396','581694'
	     )
		and json_extract_scalar(taskattributes, '$.conversations.conversation_attribute_3') is not null 
	group by 	
		cast(event_date_cst as date), 
		json_extract_scalar(taskattributes, '$.conversations.conversation_attribute_3'),
		json_extract_scalar(taskattributes, '$.conversations.conversation_attribute_6')	
), 	helix_events as (
	select distinct
	    empid as emplid,
	    helix_sessionid,
	    eventid as EventId,
		date_add('hour', -5, cast(concat(substr(eventDate, 1, 10), ' ', substr(eventDate, 12, 8)) as timestamp)) as timestamp_cst
	from
	    hive.care.l3_asurion_helix_search_all_events
	where 1=1
	    and cast(eventdatetime_cst as date) >= cast('2023-08-28' as date)
		and cast(eventdatetime_cst as date) < current_date
		and empid in (
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
	    '581145','581275','581396','581694'
		)
	    and event = 'Search_Helix_Search_Performed'    
), expert_assist_events as (
	select 
		element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') as emplid, 
		element_at(VHE.edp_raw_data_map, 'EventId') as EventId,
		cast(element_at(VHE.edp_raw_data_map, 'ExtraData_totalTime') as decimal(9, 3)) as BackendLatency,
		element_at(VHE.edp_raw_data_map, 'Identities_callSid') as Identities_callSid,
		element_at(VHE.edp_raw_data_map, 'Identities_SessionId') as Identities_SessionId,
		cast(element_at(VHE.edp_raw_data_map, 'ExtraData_openAITime') as decimal(9, 3)) as ExtraData_openAITime,
		element_at(VHE.edp_raw_data_map, 'ExtraData_botResponse') as ExtraData_botResponse,
		cast(substr(element_at(VHE.edp_raw_data_map, 'ExtraData_startRequestTime'), 1, 10) as date) as datestamp_cst,
		cast(substr(element_at(VHE.edp_raw_data_map, 'ExtraData_startRequestTime'), 1, 19) as timestamp) as timestamp_cst
	from
		l1_verizon.home_events VHE
	where 1=1
	    and element_at(VHE.edp_raw_data_map, '_header_eventContext_producer') = 'eip-ingestion-data-science'
	--    AND element_at(VHE.edp_raw_data_map, 'Identities_messageSid') IS NOT NULL
	--    AND element_at(VHE.edp_raw_data_map, 'Name') = 'RequestSummaryVoice'
	    and element_at(VHE.edp_raw_data_map, 'ExtraData_agentId') in 
	    (
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
	     '581145','581275','581396','581694'
	     )
	     and date(edp_updated_date) >= date('2023-09-25')     
), sales_map as (
	select 
		c.row_n, 
		case when sum(case when coalesce(s.eligible_flg, sn.eligible_flg) = 'true' then 1 else 0 end) > 0 then 'true' else 'false' end as eligible_flg, 
		case when sum(case when coalesce(s.accepted_flg, sn.accepted_flg) = 'true' then 1 else 0 end) > 0 then 'true' else 'false' end as accepted_flg
	from 
		calltimes c
	join 
		sales s
		on c.ucid = s.univ_call_id
		and c.ucid is not null and c.ucid != ''
	join 
		sales sn
		on c.reservation_id = sn.reservation_id
		and c.reservation_id is not null and c.reservation_id != ''
	group by c.row_n	
)
	select 
		ct.event_date_cst, ct.interaction_routing_key, ct.taskqueue, ct.direction, ct.ucid, ct.reservation_id, ct.disposition_action, ct.originating_id, ct.answering_id, 
		ct.emplid, ct.agent_type, en.expert_full_name, en.active_flg, en.tenure_group, ct.business_unit, ct.sl_acceptable, ct.abn_tm_sec, ct.ans_tm_sec, ct.interaction_tm_sec, ct.wrap_tm_sec, ct.hold_tm_sec, 
		ct.row_n, arr.quartile, arr.decile,
		eligible_flg, accepted_flg,
		count(distinct hv.EventId) as helix_eventcount, 
		count(distinct ev.EventId) as expertassist_eventcount, 
		avg(BackendLatency) as BackendLatency,
		avg(ExtraData_openAITime) as openAITime,
		max(case when tr.transfer_id is not null and tr.transfer_id != '' then 1 else 0 end) as transfer
	from 
		calltimes ct
	left join 
		callids ci
		on ct.ucid = ci.task_univ_call_id
		and ct.emplid = ci.task_emplid
	left join 
		expert_assist_events ev
		on ci.task_emplid = ev.emplid
		and ev.timestamp_cst between start_time and end_time
	left join 
		helix_events hv
		on ci.task_emplid = hv.emplid
		and hv.timestamp_cst between start_time and end_time
	left join 
		expert_names en
		on en.expert_id = ct.emplid 
	left join 
		hive.sales_analytics.l4_asurion_umt_agent_ranking_routing arr
		on cast(ct.emplid as int) = arr.emplid
		and date('2023-09-25') between arr.date_start and coalesce(arr.date_end, current_date)
		and arr.client = 'verizon'
	left join 
		sales_map sm
		on sm.row_n = ct.row_n
	left join 
		transfers tr
		on ct.ucid = tr.transfer_id 
		and ct.emplid = tr.from_agentid
	group by 
		ct.event_date_cst, ct.interaction_routing_key, ct.taskqueue, ct.direction, ct.ucid, ct.reservation_id, ct.disposition_action, ct.originating_id, ct.answering_id, 
		ct.emplid, ct.agent_type, en.expert_full_name, en.active_flg, en.tenure_group, ct.business_unit, ct.sl_acceptable, ct.abn_tm_sec, ct.ans_tm_sec, ct.interaction_tm_sec, ct.wrap_tm_sec, ct.hold_tm_sec,
		ct.row_n, arr.quartile, arr.decile, eligible_flg, accepted_flg