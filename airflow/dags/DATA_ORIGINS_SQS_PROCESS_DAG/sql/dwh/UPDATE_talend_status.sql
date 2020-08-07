UPDATE peyabi.public.talend_status 
SET date_end = convert_timezone('America/Montevideo',getdate())
, state = 'SUCCESSFUL'
WHERE date_id = to_char(convert_timezone('America/Montevideo',getdate()),'yyyymmdd')
AND job_name = '@jobName'; 