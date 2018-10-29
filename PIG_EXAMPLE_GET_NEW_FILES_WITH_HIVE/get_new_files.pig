

DEFINE GET_NEW_FILES(REFERENCE, EVENT_ID, WF_ID, UPDATE_DATE, JOB_DETAILS) RETURNS NEW_FILES {


----- LOADING REFERENCE FILES FOR IDENTIFYING THE NEW INPUT FILES  -----

--dump REFERENCE;


reference = LOAD '$REFERENCE' USING PigStorage(',', '-tagFile') AS (ref_file_name:chararray, file_path:chararray, processed_time:chararray);

reference = FOREACH reference GENERATE ref_file_name, REGEX_EXTRACT(file_path, '.*/(.*)', 1) AS file_name, file_path, processed_time;

reference_distinct = FOREACH (GROUP reference BY file_name)  {

    result = TOP(1, 0, $1);
    GENERATE FLATTEN(result);

}

reference_distinct = FOREACH reference_distinct GENERATE $0 as ref_file_name:chararray,$1 as file_name:chararray,$2 as file_path:chararray,$3 as processed_time:chararray;

----- LOADING PROCESSED FILES' DETAILS -----

job_details = LOAD '$JOB_DETAILS' USING org.apache.hive.hcatalog.pig.HCatLoader();

job_details = FILTER (GROUP job_details BY event_id) BY (group == '$EVENT_ID');

job_details = FOREACH job_details GENERATE FLATTEN(job_details.file_name) AS file_name, MAX(job_details.processed_time) AS latest_processed_time;

--dump reference_distinct;

---- MAPPING NEW FILES FROM REFERENCE FILE WITH PROCESSED FILES

ref_processed = FOREACH (JOIN reference_distinct BY file_name LEFT, job_details BY file_name) GENERATE reference_distinct::ref_file_name AS ref_file_name, reference_distinct::file_path AS file_path, reference_distinct::file_name AS file_name, reference_distinct::processed_time AS processed_time, job_details::file_name AS job_details_file_name, (latest_processed_time IS NOT NULL ? latest_processed_time : '0') AS latest_processed_time;

ref_processed = FILTER ref_processed BY (processed_time > latest_processed_time);


----- IDENTIFYING AND STORING DUPLICATE FILES -----

duplicate = FOREACH (FILTER ref_processed BY (file_name IS NOT NULL AND job_details_file_name IS NOT NULL)) GENERATE ref_file_name, file_path, file_name, processed_time
, '$WF_ID' AS wf_id, '$UPDATE_DATE' AS update_date, 'DUPLICATE_FILE' AS reason;

STORE duplicate INTO '$JOB_DETAILS' USING org.apache.hive.hcatalog.pig.HCatStorer('event_id=$EVENT_ID');


----- RETURNING NEW FILES -----

$NEW_FILES = FOREACH (FILTER ref_processed BY (job_details_file_name IS NULL)) GENERATE ref_file_name, file_path AS src_file_path, file_name AS src_file_name, processed_time AS src_processed_time;

};
