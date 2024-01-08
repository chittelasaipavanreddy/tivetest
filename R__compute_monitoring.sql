
create or replace schema COMPUTE_CREDIT_MONITOR_SCHEMA;

create or replace TABLE EDITIONS (
	EDITION_NAME VARCHAR(16777216),
	CREDITS FLOAT
);

create or replace TABLE TABLE_MONITOR (
	DATE TIMESTAMP_NTZ(9),
	ACCOUNT_ID VARCHAR(16777216),
	ORG_ID VARCHAR(16777216),
	WAREHOUSE_NAME VARCHAR(16777216),
	USER_NAME VARCHAR(16777216),
	USER_EMAIL VARCHAR(16777216),
	WAREHOUSE_SIZE VARCHAR(16777216),
	EXECUTION_TIME NUMBER(38,0),
	SESSION_ID NUMBER(38,0),
	QUERY_ID VARCHAR(16777216),
	STMT_CNT NUMBER(38,0),
	ESTIMATED_CREDITS FLOAT
);

create or replace schema MAPPING;

create or replace TABLE ACCOUNTID_ACCTNAME (
	ACCOUNT_ID VARCHAR(256),
	ACCOUNT_NAME VARCHAR(256)
);
create or replace TABLE ORGID_ACCOUNT_ID (
	ORGID VARCHAR(256),
	ACCOUNT_ID VARCHAR(256),
	ESTIMATED_CREDITS NUMBER(38,0)
);
create or replace TABLE ORGID_ORGNAME (
	ORG_ID VARCHAR(16777216),
	COMPANY_NAME VARCHAR(200)
);

create or replace view COMPUTE_CREDIT_MONITOR_SCHEMA.V_TABLE_MONITOR(
	DATE,
	QUERY_EXEC_DATE,
	ACCOUNT_ID,
	ACCOUNT_NAME,
	ORG_ID,
	ORG_NAME,
	WAREHOUSE_NAME,
	USER_NAME,
	USER_EMAIL,
	WAREHOUSE_SIZE,
	EXECUTION_TIME,
	SESSION_ID,
	QUERY_ID,
	STMT_CNT,
	ESTIMATED_CREDITS
) as
select DATE, date(DATE) as QUERY_EXEC_DATE,ACCOUNT_ID,account_name, ORG_ID,org_name, WAREHOUSE_NAME, USER_NAME, USER_EMAIL, WAREHOUSE_SIZE, EXECUTION_TIME, SESSION_ID, QUERY_ID, STMT_CNT, ESTIMATED_CREDITS from
(select DATE,tm.ACCOUNT_ID,a.account_name,tm.ORG_ID,o.company_name as org_name,WAREHOUSE_NAME,USER_NAME,USER_EMAIL,WAREHOUSE_SIZE,EXECUTION_TIME,SESSION_ID,QUERY_ID,STMT_CNT,ESTIMATED_CREDITS from KIPI_WATCHKEEPER.COMPUTE_CREDIT_MONITOR_SCHEMA.TABLE_MONITOR tm join KIPI_WATCHKEEPER.MAPPING.ORGID_ORGNAME o
on tm.org_id = o.ORG_ID
join KIPI_WATCHKEEPER.MAPPING.ACCOUNTID_ACCTNAME a 
on tm.account_id = a.account_id)
;

CREATE OR REPLACE PROCEDURE COMPUTE_CREDIT_MONITOR_SCHEMA."SP_MONITOR"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

snowflake.execute( {sqlText: `
delete from KIPI_WATCHKEEPER.COMPUTE_CREDIT_MONITOR_SCHEMA.TABLE_MONITOR 
where date = (Select max(date) 
from KIPI_WATCHKEEPER.COMPUTE_CREDIT_MONITOR_SCHEMA.TABLE_MONITOR);`} );
 
var sql_command = `
insert into "KIPI_WATCHKEEPER"."COMPUTE_CREDIT_MONITOR_SCHEMA".TABLE_MONITOR
(DATE,ACCOUNT_ID,ORG_ID,WAREHOUSE_NAME,USER_NAME,USER_EMAIL,WAREHOUSE_SIZE,EXECUTION_TIME,SESSION_ID,QUERY_ID,STMT_CNT,ESTIMATED_CREDITS)
SELECT  convert_timezone(''UTC'', START_TIME)::datetime as date,
SPLIT_PART(REGEXP_SUBSTR(query_text, ''account_id:([^,]+)''),'':'',2) AS account_id,
SPLIT_PART(REGEXP_SUBSTR(query_text, ''org_id:([^,]+)''),'':'',2) AS org_id,
warehouse_name,
user_name,
SPLIT_PART(REGEXP_SUBSTR(query_text, ''user_email:([^,]+)''),'':'',2) AS user_email,
warehouse_size,
execution_time,
session_id,	
query_id,
count(*) as stmt_cnt,			
sum(execution_time/1000 *			
case warehouse_size			
when ''X-Small'' then 1/60/60			
when ''Small''   then 2/60/60			
when ''Medium''  then 4/60/60			
when ''Large''   then 8/60/60			
when ''X-Large'' then 16/60/60			
when ''2X-Large'' then 32/60/60			
when ''3X-Large'' then 64/60/60			
when ''4X-Large'' then 128/60/60		
else 0				
end) as estimated_credits
from (select * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text LIKE ''%--service_name%''  and user_name like ''PLATFORM_ANALYTICS_USER%'' ) 
WHERE convert_timezone(''UTC'', DATE) > (select max(convert_timezone(''UTC'', DATE)) from KIPI_WATCHKEEPER.compute_credit_monitor_schema.TABLE_MONITOR)
group by 1,2,3,4,5,6,7,8,9,10
order by 1 desc,4 desc,2;`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
';

create or replace task COMPUTE_CREDIT_MONITOR_SCHEMA.TABLE_MONITOR_TASK
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 3 * * * UTC'
	as call SP_MONITOR();
