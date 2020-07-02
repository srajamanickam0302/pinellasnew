{{
config (	materialized='incremental',
            unique_key='CATEGORY_SK'
)
}}

with source_data as (
  SELECT             CATEGORY_ID
                    ,CATEGORY_NAME
                    ,CAT_LEVEL0
                    ,CAT_LEVEL1
                    ,MAJOR_CATNAME


  FROM  {{ref('category_source')}}
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
   ),

nw_record as
				(select a.CATEGORY_ID
                        , a.CAT_LEVEL0 AS CATEGORY_LEVEL0
                        , a.CAT_LEVEL1 AS CATEGORY_LEVEL1
						, a.CATEGORY_NAME AS CATEGORY_NAME
                        , a.MAJOR_CATNAME AS MAJOR_CATEGORYNAME
                        , '' AS CATEGORY_LEVEL4
                        , '' AS CATEGORY_LEVEL5
                        , '' AS CATEGORY_LEVEL6
                        ,	to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        ,	null AS UPDATED_DATETIME
                        , (SELECT CASE WHEN C_SK IS NULL THEN 0 ELSE C_SK END from (select max(CATEGORY_SK) AS C_SK FROM fivetran.information_layer.T_DIM_CATEGORY ) A )
                			  + row_number() over (order by a.CATEGORY_ID,a.CATEGORY_NAME desc) AS CATEGORY_SK
				from source_data  a
				where not exists (select 1 from fivetran.information_layer.T_DIM_CATEGORY b where a.CATEGORY_ID=b.CATEGORY_ID AND TRIM(a.CATEGORY_NAME) = TRIM(b.CATEGORY_NAME))
					),

ex_record as
				(select             b.CATEGORY_SK
                        , a.CATEGORY_ID
                        , a.CAT_LEVEL0 AS CATEGORY_LEVEL0
                        , a.CAT_LEVEL1 AS CATEGORY_LEVEL1
						, a.CATEGORY_NAME AS CATEGORY_NAME
                        , a.MAJOR_CATNAME AS MAJOR_CATEGORYNAME
                        , '' AS CATEGORY_LEVEL4
                        , '' AS CATEGORY_LEVEL5
                        , '' AS CATEGORY_LEVEL6
						, b.CREATED_DATETIME
                        , to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join fivetran.information_layer.T_DIM_CATEGORY b on ( a.CATEGORY_ID=b.CATEGORY_ID AND TRIM(a.CATEGORY_NAME) = TRIM(b.CATEGORY_NAME) )
				),
final as
(
select
        CATEGORY_SK
      ,	CATEGORY_ID
      ,	CATEGORY_LEVEL0
	  , CATEGORY_LEVEL1
      ,	CATEGORY_NAME
      ,	MAJOR_CATEGORYNAME
      ,	CATEGORY_LEVEL4
      ,	CATEGORY_LEVEL5
      ,	CATEGORY_LEVEL6
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
from nw_record
union all
select
        CATEGORY_SK
      ,	CATEGORY_ID
      ,	CATEGORY_LEVEL0
	  , CATEGORY_LEVEL1
      ,	CATEGORY_NAME
      ,	MAJOR_CATEGORYNAME
      ,	CATEGORY_LEVEL4
      ,	CATEGORY_LEVEL5
      ,	CATEGORY_LEVEL6
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final
