{{ config(materialized='ephemeral') }}
    SELECT
    SEGMENT1               CONTRACT_NUMBER,
    COMMENTS               CONTRACT_DESCRIPTION,
    BLANKET_TOTAL_AMOUNT   CONTRACT_AMOUNT,
    START_DATE             CONTRACT_START_DATE,
    END_DATE               CONTRACT_END_DATE,
	PO_HEADER_ID CONTRACT_ID   /* Oracle EBS Natural Key */
FROM
    "FIVETRAN"."ORACLE_EBS_PO".PO_HEADERS_ALL
WHERE
    TYPE_LOOKUP_CODE = 'CONTRACT'