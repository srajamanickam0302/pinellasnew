{{ config(materialized='ephemeral') }}
       SELECT
    SEGMENT1        PO_NUMBER,
    COMMENTS        PO_DESC,
    CREATION_DATE   PO_DATE,
    PO_AMOUNT,
    PO_HEADERS_ALL.PO_HEADER_ID  /* Oracle EBS Natural Key */
FROM
     "FIVETRAN"."ORACLE_EBS_PO".PO_HEADERS_ALL,
    (
        SELECT
            PO_HEADER_ID,
            SUM(DECODE(PURCHASE_BASIS, 'GOODS', (QUANTITY * UNIT_PRICE), 'AMOUNT', 0)) PO_AMOUNT
        FROM
            "FIVETRAN"."ORACLE_EBS_PO". PO_LINES_ALL
        WHERE
            ( CANCEL_FLAG IS NULL
              OR CANCEL_FLAG = 'N' )
        GROUP BY
            PO_HEADER_ID
    ) PO_AMT
WHERE
    TYPE_LOOKUP_CODE = 'STANDARD'
    AND PO_HEADERS_ALL.PO_HEADER_ID = PO_AMT.PO_HEADER_ID