{{
config (	materialized='incremental',
            unique_key='CONTRACT_SK'
)
}}

with source_data as (
  SELECT             CONTRACT_NUMBER
                    ,CONTRACT_DESCRIPTION
                    ,CONTRACT_AMOUNT
                    ,CONTRACT_START_DATE
					,CONTRACT_END_DATE
					,CONTRACT_ID
 FROM  {{ref('contract_source')}}
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
   ),

nw_record as
				(select   a.CONTRACT_NUMBER
                        , a.CONTRACT_DESCRIPTION
                        , a.CONTRACT_AMOUNT
                        , a.CONTRACT_START_DATE 
						, a.CONTRACT_END_DATE
						, a.CONTRACT_ID
                        , to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        , null AS UPDATED_DATETIME
                        , (SELECT CASE WHEN CONTRACT_SK IS NULL THEN 0 ELSE CONTRACT_SK END from (select max(CONTRACT_SK) AS CONTRACT_SK FROM fivetran.information_layer.T_DIM_CONTRACT ) A )
                			  + row_number() over (order by a.CONTRACT_NUMBER,a.CONTRACT_DESCRIPTION desc) AS CONTRACT_SK
				from source_data  a
				where not exists (select 1 from fivetran.information_layer.T_DIM_CONTRACT b where a.CONTRACT_NUMBER=b.CONTRACT_NUMBER AND TRIM(a.CONTRACT_DESCRIPTION) = TRIM(b.CONTRACT_DESCRIPTION))
					),

ex_record as
				(select   b.CONTRACT_SK
                        , a.CONTRACT_NUMBER
                        , a.CONTRACT_DESCRIPTION
                        , a.CONTRACT_AMOUNT
                        , a.CONTRACT_START_DATE
						, a.CONTRACT_END_DATE
						, a.CONTRACT_ID						
                        , b.CREATED_DATETIME
                        , to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join fivetran.information_layer.T_DIM_CONTRACT b on ( a.CONTRACT_NUMBER=b.CONTRACT_NUMBER AND TRIM(a.CONTRACT_DESCRIPTION) = TRIM(b.CONTRACT_DESCRIPTION) )
				),
final as
(
select
        CONTRACT_SK
      ,	CONTRACT_NUMBER
      ,	CONTRACT_DESCRIPTION
      ,	CONTRACT_AMOUNT
      ,	CONTRACT_START_DATE
	  , CONTRACT_END_DATE
	  , CONTRACT_ID	
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from nw_record
union all
select
        CONTRACT_SK
      ,	CONTRACT_NUMBER
      ,	CONTRACT_DESCRIPTION
      ,	CONTRACT_AMOUNT
      ,	CONTRACT_START_DATE
	  , CONTRACT_END_DATE
	  , CONTRACT_ID	
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final
