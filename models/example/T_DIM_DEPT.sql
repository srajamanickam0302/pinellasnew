{{
config (	materialized='incremental',
            unique_key='DEPT_SK'
)
}}

with source_data as (
  SELECT             DEPARTMENT_CODE       AS DEPT_ID
        		      	,DEPARTMENT_DESC       AS DEPT_NAME
  FROM  staging.s01_stage_source_1
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
  GROUP BY 1,2
   ),

nw_record as
				(select           a.DEPT_ID
                        ,	a.DEPT_NAME AS	DEPT_NAME
                        , NULL AS	DEPT_LEVEL1
                        , NULL AS	DEPT_LEVE2
                        , NULL AS	DEPT_LEVEL3
                        , NULL AS	DEPT_LEVEL4
                        , NULL AS	DEPT_LEVEL5
                        , NULL AS	DEPT_LEVEL6
                        , NULL AS	DEPT_LEVEL7
                        , NULL AS	CLASSIFICATION
                        , NULL AS	DEPT_HEAD
                        , NULL AS	CONTROL_CENTER
                        , NULL AS	PHONE
                        , NULL AS	WEBSITE
                        ,	to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        ,	null AS UPDATED_DATETIME
                        ,  (SELECT CASE WHEN D_SK IS NULL THEN 0 ELSE D_SK END from (select max(DEPT_SK) AS D_SK FROM PINELLAS.STAGING.T_DIM_DEPT ) A )
                			  + row_number() over (order by a.DEPT_ID,a.DEPT_NAME desc) AS DEPT_SK
				from source_data  a
				where not exists (select 1 from STAGING.T_DIM_DEPT b where a.DEPT_ID=b.DEPT_ID AND TRIM(a.DEPT_NAME) = TRIM(b.DEPT_NAME))
					),

ex_record as
				(select             b.DEPT_SK
                          , a.DEPT_ID
                          ,	a.DEPT_NAME AS	DEPT_NAME
                          , NULL AS	DEPT_LEVEL1
                          , NULL AS	DEPT_LEVE2
                          , NULL AS	DEPT_LEVEL3
                          , NULL AS	DEPT_LEVEL4
                          , NULL AS	DEPT_LEVEL5
                          , NULL AS	DEPT_LEVEL6
                          , NULL AS	DEPT_LEVEL7
                          , NULL AS	CLASSIFICATION
                          , NULL AS	DEPT_HEAD
                          , NULL AS	CONTROL_CENTER
                          , NULL AS	PHONE
                          , NULL AS	WEBSITE
                          ,	b.CREATED_DATETIME
                          ,	to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join STAGING.T_DIM_DEPT b on ( a.DEPT_ID=b.DEPT_ID AND TRIM(a.DEPT_NAME) = TRIM(b.DEPT_NAME) )
				),
final as
(
select
          DEPT_SK
        ,	DEPT_ID
        ,	DEPT_NAME
        ,	DEPT_LEVEL1
        ,	DEPT_LEVE2
        ,	DEPT_LEVEL3
        ,	DEPT_LEVEL4
        ,	DEPT_LEVEL5
        ,	DEPT_LEVEL6
        ,	DEPT_LEVEL7
        ,	CLASSIFICATION
        ,	DEPT_HEAD
        ,	CONTROL_CENTER
        ,	PHONE
        ,	WEBSITE
        ,	CREATED_DATETIME
        ,	UPDATED_DATETIME
from nw_record
union all
select
          DEPT_SK
        ,	DEPT_ID
        ,	DEPT_NAME
        ,	DEPT_LEVEL1
        ,	DEPT_LEVE2
        ,	DEPT_LEVEL3
        ,	DEPT_LEVEL4
        ,	DEPT_LEVEL5
        ,	DEPT_LEVEL6
        ,	DEPT_LEVEL7
        ,	CLASSIFICATION
        ,	DEPT_HEAD
        ,	CONTROL_CENTER
        ,	PHONE
        ,	WEBSITE
        ,	CREATED_DATETIME
        ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final