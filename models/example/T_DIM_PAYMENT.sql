{{
config (	materialized='incremental',
            unique_key='PAYMENT_SK'
)
}}

with source_data as (
  SELECT             PAYMENT_CODE
                    ,CHEQUE_NUMBER
                    ,CHECK_DATE
                    ,ACCOUNTING_DATE
					,CHECK_ID
 FROM  {{ref('payment_source')}}
  -- Need to remove the where clause when the data issue will be fixed.
  {% if is_incremental() %}
  WHERE 1 = 1
  {% endif %}
   ),

nw_record as
				(select   a.PAYMENT_CODE as PAYMENT_MODE
                        , a.CHEQUE_NUMBER
                        , a.CHECK_DATE
                        , a.ACCOUNTING_DATE 
						, a.CHECK_ID
                        , to_timestamp_ntz(current_timestamp) as CREATED_DATETIME
                        , null AS UPDATED_DATETIME
                        , (SELECT CASE WHEN PAYMENT_SK IS NULL THEN 0 ELSE PAYMENT_SK END from (select max(PAYMENT_SK) AS PAYMENT_SK FROM fivetran.information_layer.T_DIM_PAYMENT ) A )
                			  + row_number() over (order by a.CHEQUE_NUMBER desc) AS PAYMENT_SK
				from source_data  a
				where not exists (select 1 from fivetran.information_layer.T_DIM_PAYMENT b where a.CHEQUE_NUMBER=b.CHEQUE_NUMBER)
					),

ex_record as
				(select   b.PAYMENT_SK
                        , a.PAYMENT_CODE as PAYMENT_MODE
                        , a.CHEQUE_NUMBER
                        , a.CHECK_DATE
                        , a.ACCOUNTING_DATE 
						, a.CHECK_ID				
                        , b.CREATED_DATETIME
                        , to_timestamp_ntz(current_timestamp)  AS UPDATED_DATETIME
					from source_data  a
					join fivetran.information_layer.T_DIM_PAYMENT b on ( a.CHEQUE_NUMBER=b.CHEQUE_NUMBER )
				),
final as
(
select
        CONTRACT_SK
      ,	PAYMENT_MODE
      ,	CHEQUE_NUMBER
      ,	CHECK_DATE
      ,	ACCOUNTING_DATE
	  , CHECK_ID
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from nw_record
union all
select
        CONTRACT_SK
      ,	PAYMENT_MODE
      ,	CHEQUE_NUMBER
      ,	CHECK_DATE
      ,	ACCOUNTING_DATE
	  , CHECK_ID
      ,	CREATED_DATETIME
      ,	UPDATED_DATETIME
  from ex_record
  )
  select
	 *  from final
