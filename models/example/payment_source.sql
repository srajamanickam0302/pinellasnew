{{ config(materialized='ephemeral') }}    
    SELECT
    NVL((
        SELECT
            DESCRIPTION
        FROM
            AP_LOOKUP_CODES
        WHERE
            LOOKUP_TYPE IN(
                'EDI PAYMENT FORMAT', 'PAYMENT METHOD'
            )
            AND LOOKUP_CODE = AC.PAYMENT_METHOD_CODE
    ), AC.PAYMENT_METHOD_CODE) PAYMENT_CODE,
    AC.CHECK_NUMBER CHEQUE_NUMBER,
    AC.CHECK_DATE,
    NULL ACCOUNTING_DATE,
	AC.CHECK_ID  /* Oracle EBS Natural Key */
FROM
     "FIVETRAN"."ORACLE_EBS_AP".AP_CHECKS_ALL AC