DELETE FROM public.talend_status
WHERE date_id = to_char(convert_timezone('America/Montevideo',getdate()),'yyyymmdd')
AND job_name = '@jobName';

INSERT INTO peyabi.public.talend_status (date_id, job_name,full_date,date_start,visible) 
VALUES (CAST(to_char(convert_timezone('America/Montevideo',getdate()),'yyyymmdd') as integer), '@jobName',convert_timezone('America/Montevideo',getdate()),convert_timezone('America/Montevideo',getdate()),@visible); 
