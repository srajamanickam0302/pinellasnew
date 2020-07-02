{{
config (	materialized='incremental',
            unique_key='PO_SK'
)
}}

with source_data as (
  SELECT             PO_NUMBER
                    ,PO_DESC
                    ,PO_DATE
					,PO_AMOUNT
                    ,PO_HEADER_ID
 FROM  {{ref('purchase_order_source')}}
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
   ),

nw_record as
				(select   a.PO_NUMBER
                        , a.PO_DESC
                        , a.PO_DATE
						, a.PO_AMOUNT
                        , a.PO_HEADER_ID 
                        , to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        , null AS UPDATED_DATETIME
                        , (SELECT CASE WHEN PO_SK IS NULL THEN 0 ELSE PO_SK END from (select max(PO_SK) AS PO_SK FROM fivetran.information_layer.T_DIM_PURCHASE_ORDER ) A )
                			  + row_number() over (order by a.PO_NUMBER,a.PO_DESC desc) AS PO_SK
				from source_data  a
				where not exists (select 1 from fivetran.information_layer.T_DIM_PURCHASE_ORDER b where a.PO_NUMBER=b.PO_NUMBER AND TRIM(a.PO_DESC) = TRIM(b.PO_DESC))
					),

ex_record as
				(select     b.PO_SK
                          , a.PO_NUMBER
                          ,	a.PO_DESC
                          , a.PO_DATE
						  , a.PO_AMOUNT
                          , a.PO_HEADER_ID
                          ,	b.CREATED_DATETIME
                          ,	to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join fivetran.information_layer.T_DIM_PURCHASE_ORDER b on ( a.PO_NUMBER=b.PO_NUMBER AND TRIM(a.PO_DESC) = TRIM(b.PO_DESC) )
				),
final as
(
select
        PO_SK
      ,	PO_NUMBER
      ,	PO_DESC
      ,	PO_DATE
	  , PO_AMOUNT
      ,	PO_HEADER_ID
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from nw_record
union all
select
         PO_SK
      ,	PO_NUMBER
      ,	PO_DESC
      ,	PO_DATE
	  , PO_AMOUNT
      ,	PO_HEADER_ID
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final
