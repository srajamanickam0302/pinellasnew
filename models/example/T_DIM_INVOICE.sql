{{
config (	materialized='incremental',
            unique_key='INVOICE_SK'
)
}}

with source_data as (
  SELECT             INVOICE_NUMBER
                    ,INVOICE_DESCRIPTION
                    ,INVOICE_DATE
                    ,INVOICE_ID
 FROM  {{ref('invoice_source')}}
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
   ),

nw_record as
				(select   a.INVOICE_NUMBER
                        , a.INVOICE_DESCRIPTION
                        , a.INVOICE_DATE
                        , a.INVOICE_ID 
                        , to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        , null AS UPDATED_DATETIME
                        , (SELECT CASE WHEN INVOICE_SK IS NULL THEN 0 ELSE INVOICE_SK END from (select max(INVOICE_SK) AS INVOICE_SK FROM fivetran.information_layer.T_DIM_INVOICE ) A )
                			  + row_number() over (order by a.INVOICE_NUMBER,a.INVOICE_DESCRIPTION desc) AS INVOICE_SK
				from source_data  a
				where not exists (select 1 from fivetran.information_layer.T_DIM_INVOICE b where a.INVOICE_NUMBER=b.INVOICE_NUMBER AND TRIM(a.INVOICE_DESCRIPTION) = TRIM(b.INVOICE_DESCRIPTION))
					),

ex_record as
				(select   b.INVOICE_SK
                        , a.INVOICE_NUMBER
                        , a.INVOICE_DESCRIPTION
                        , a.INVOICE_DATE
                        , a.INVOICE_ID 
                        ,	b.CREATED_DATETIME
                        ,	to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join fivetran.information_layer.T_DIM_INVOICE b on ( a.INVOICE_NUMBER=b.INVOICE_NUMBER AND TRIM(a.INVOICE_DESCRIPTION) = TRIM(b.INVOICE_DESCRIPTION) )
				),
final as
(
select
        INVOICE_SK
      ,	INVOICE_NUMBER
      ,	INVOICE_DESCRIPTION
      ,	INVOICE_DATE
      ,	INVOICE_ID
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from nw_record
union all
select
        INVOICE_SK
      ,	INVOICE_NUMBER
      ,	INVOICE_DESCRIPTION
      ,	INVOICE_DATE
      ,	INVOICE_ID
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final
