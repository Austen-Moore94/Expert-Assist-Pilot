SELECT
    mo.emplid as "Employee",
    cast(mo.metric_date as date) as "Date",
    SUM(CASE WHEN mo."metric_name" = 'crt_num' THEN COALESCE("Value",0) ELSE 0 END) Total_Resolution_Time,
    SUM(CASE WHEN mo."metric_name" = 'crt_den' THEN COALESCE("Value",0) ELSE 0 END) Sessions_Resolved,
    SUM(CASE WHEN mo."metric_name" = 'total_calls' THEN COALESCE("Value",0) ELSE 0 END) TotalCalls
FROM care.metric_object mo
WHERE 1=1
    and mo.business_unit = 'soluto'
    and mo."aggregate"= 'daily'
    and mo."channel"!='messaging'
    AND mo."metric_name" in ('crt_num','crt_den','total_calls')
    AND mo.emplid != '-1'
GROUP BY
    mo.emplid,
    cast(mo.metric_date as date)