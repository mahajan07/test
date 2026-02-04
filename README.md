/* 1 unified table for “who called / when / how many times”
   - Keeps your crucial de-dupe logic via QUALIFY + window ranking
   - Cleans member number so 12345.0 -> 12345 (and keeps only digits)
   - Ready to UNION the 2023–Dec2024 (TASK) + Jan2025–last month (VOICECALL) datasets
*/

WITH params AS (
  SELECT
    TO_DATE('2023-01-01')                                  AS old_start_dt,
    TO_DATE('2024-12-31')                                  AS old_end_dt,
    TO_DATE('2025-01-01')                                  AS new_start_dt,
    DATEADD(day, -1, DATE_TRUNC('month', CURRENT_DATE()))   AS new_end_dt   -- end of last month
),

/* ---------- OLD SOURCE (2023–Dec 2024) : TASK ---------- */
task_calls AS (
  SELECT
    /* duration is not present in TASK -> keep NULL (or map if you have a duration field elsewhere) */
    NULL::NUMBER                                           AS call_duration_seconds,

    /* member formula cleanup: digits only + strip trailing .0 if present */
    REGEXP_SUBSTR(
      REGEXP_REPLACE(TO_VARCHAR(cse.member_number_formula), '\\.0+$', ''),
      '\\d+'
    )                                                      AS member_formula,

    /* call time */
    tsk.ActivityDate::TIMESTAMP_NTZ                         AS call_start_time,

    /* call type */
    tsk.CallType                                            AS call_type,

    /* task id */
    tsk.Task_ID::VARCHAR                                    AS call_interaction_or_task_id
  FROM (
    SELECT
      id                                                    AS Task_ID,
      CallType,
      createddate,
      ActivityDate,
      account,
      primary_reason,
      type,
      record_last_updated,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY record_last_updated DESC) AS rnk
    FROM PENFED_PROD.raw_salesforce.task
    WHERE type ILIKE '%Call%'
      AND COALESCE(primary_reason,'') <> ''
      AND primary_reason <> 'Other Inquiry'
      AND TO_DATE(ActivityDate) BETWEEN (SELECT old_start_dt FROM params) AND (SELECT old_end_dt FROM params)
    QUALIFY rnk = 1
  ) tsk
  LEFT JOIN (
    SELECT DISTINCT
      accountid,
      member_number_formula
    FROM raw_salesforce.casedata
    WHERE accountid IS NOT NULL
      AND accountid <> ''
      AND member_number_formula IS NOT NULL
      AND member_number_formula <> ''
  ) cse
    ON tsk.account = cse.accountid
),

/* ---------- NEW SOURCE (Jan 2025–last month) : VOICECALL ---------- */
voice_calls AS (
  SELECT
    A.CALLDURATIONINSECONDS__C::NUMBER                      AS call_duration_seconds,

    REGEXP_SUBSTR(
      REGEXP_REPLACE(TO_VARCHAR(COALESCE(C.MEMBER_NUMBER_FORMULA__C__C, A.MEMBERNUMBER__C__C)), '\\.0+$', ''),
      '\\d+'
    )                                                      AS member_formula,

    A.CALLSTARTDATETIME__C::TIMESTAMP_NTZ                   AS call_start_time,

    A.CALLTYPE__C                                           AS call_type,

    COALESCE(A.INTERACTIONID__C__C, B.ID__C, A.ID__C)::VARCHAR AS call_interaction_or_task_id
  FROM PENFED_PROD_SALESFORCE.RAW_OPS.VOICECALL A
  LEFT JOIN PENFED_PROD_SALESFORCE.RAW_OPS.ENGAGEMENT_INTERACTION B
    ON A.ENGAGEMENT_INTERACTION__C__C = B.ID__C
  LEFT JOIN PENFED_PROD_SALESFORCE.RAW_OPS.CASE C
    ON A.CASE__C__C = C.ID__C
  WHERE A.CREATEDDATE__C BETWEEN (SELECT new_start_dt FROM params) AND (SELECT new_end_dt FROM params)
    AND A.CALLDISPOSITION__C = 'completed'
    AND (A.QUEUE_NAME__C ILIKE '%MSS%' OR A.QUEUE_NAME__C ILIKE '%MES%')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A.NAME__C ORDER BY A.LASTMODIFIEDDATE__C DESC) = 1
)

/* ---------- FINAL UNION TABLE ---------- */
SELECT
  call_duration_seconds,
  member_formula,
  call_start_time,
  call_type,
  call_interaction_or_task_id
FROM task_calls

UNION ALL

SELECT
  call_duration_seconds,
  member_formula,
  call_start_time,
  call_type,
  call_interaction_or_task_id
FROM voice_calls
;
