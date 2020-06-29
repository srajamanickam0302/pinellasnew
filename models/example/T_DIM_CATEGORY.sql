{{
config (	materialized='incremental',
            unique_key='CAT_SK'
)
}}

with source_data as (
  SELECT                         SEGMENT3       AS CAT_ID
        		      	,DEPARTMENT_DESC       AS CAT_NAME
  FROM  staging.s01_stage_source_1
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
  GROUP BY 1,2
   ),

nw_record as
	      (select           a.SEGMENT3 AS CAT_ID
                        ,	a.CAT_NAME AS	CAT_NAME
                        , NULL AS	CAT_LEVEL1
                        , NULL AS	MAJOR_CATNAME
                        , NULL AS	DIVISION
                        ,to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        ,null AS UPDATED_DATETIME
                        ,  (SELECT CASE WHEN C_SK IS NULL THEN 0 ELSE C_SK END from (select max(CAT_SK) AS C_SK FROM PINELLAS.STAGING.T_DIM_CATEGORY ) A )
                			  + row_number() over (order by a.CAT_ID,a.CAT_NAME desc) AS CAT_SK
				from source_data  a
				where not exists (select 1 from STAGING.T_DIM_CATEGORY b where a.CAT_ID=b.CAT_ID AND TRIM(a.CAT_NAME) = TRIM(b.CAT_NAME))
					),

ex_record as
				(select             b.CAT_SK
                          , a.CAT_ID
                          ,	a.CAT_NAME AS	CAT_NAME
                          , NULL AS	CAT_LEVEL1
                          , NULL AS	MAJOR_CATNAME
                          , NULL AS	DIVISION
                          ,b.CREATED_DATETIME
                          ,to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join STAGING.T_DIM_CATEGORY b on ( a.CAT_ID=b.CAT_ID AND TRIM(a.CAT_NAME) = TRIM(b.CAT_NAME) )
				),
final as
(
select
          CAT_SK
        ,	CAT_ID
        ,	CAT_NAME
        ,	CAT_LEVEL1
        ,	MAJOR_CATNAME
        ,	DIVISION
        ,	CREATED_DATETIME
        ,	UPDATED_DATETIME
from nw_record
union all
select
          CAT_SK
        ,	CAT_ID
        ,	CAT_NAME
        ,	CAT_LEVEL1
        ,	MAJOR_CATNAME
        ,	DIVISION
        ,	CREATED_DATETIME
        ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final