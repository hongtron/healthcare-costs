REGISTER /usr/local/mpcs53013/elephant-bird-core-4.14.jar;
REGISTER /usr/local/mpcs53013/elephant-bird-pig-4.14.jar;
REGISTER /usr/local/mpcs53013/elephant-bird-hadoop-compat-4.14.jar;
REGISTER /usr/local/mpcs53013/libthrift-0.9.0.jar;
register /home/mpcs53013/alih/alih-HdfsIngestProcedures-0.0.1-SNAPSHOT.jar;
REGISTER /usr/local/mpcs53013/piggybank.jar;

DEFINE PSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('edu.uchicago.mpcs53013.alih.HdfsIngestProcedure.ProcedureSummary');

RAW_FINANCIAL_DATA = LOAD '/inputs/alih/financial/*' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER');
FINANCIAL_DATA = FOREACH RAW_FINANCIAL_DATA GENERATE $2 AS zip, $3 AS agi_range, $4 as num_returns, $14 AS agi, $16 as total_income;

FINANCIAL_DATA_FILTERED = FILTER FINANCIAL_DATA BY (agi neq 0) AND (total_income neq 0);
FINANCIAL_DATA_CAST = FOREACH FINANCIAL_DATA_FILTERED GENERATE zip, agi_range, num_returns, (long)agi, (long)total_income;
FINANCIAL_DATA_TOTALS = FOREACH FINANCIAL_DATA_CAST GENERATE zip, agi_range, num_returns,
-- The data set provides average values, so estimate total $ amounts to new data to be incorporated in the speed layer
  (agi * num_returns) as total_agi:long,
  (total_income * num_returns) as total_total_income:long;

RAW_PROCEDURE_DATA = LOAD '/inputs/alih/thriftProceduresSummary/proceduresSummary' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
PROCEDURE_SUMMARY = FOREACH RAW_PROCEDURE_DATA GENERATE FLATTEN(PSThriftBytesToTuple(value));
PROCEDURE_SUMMARY_FILTERED = FILTER PROCEDURE_SUMMARY BY (ProcedureSummary::zip neq '') AND (ProcedureSummary::state neq '') AND
  (ProcedureSummary::avgCoveredCharges neq 0) AND (ProcedureSummary::avgTotalPayments neq 0) AND (ProcedureSummary::avgMedicarePayments neq 0);
PROCEDURE_SUMMARY_TOTALS = FOREACH PROCEDURE_SUMMARY_FILTERED GENERATE ProcedureSummary::drgCode as drg_code, ProcedureSummary::drgName as drg_name, ProcedureSummary::providerName as provider_name,
  ProcedureSummary::zip as provider_zip, ProcedureSummary::state as state, ProcedureSummary::numDischarges as num_discharges,
  -- The data set provides average values, so estimate total $ amounts to new data to be incorporated in the speed layer
  (ProcedureSummary::avgCoveredCharges * ProcedureSummary::numDischarges) as covered_charges,
  (ProcedureSummary::avgTotalPayments * ProcedureSummary::numDischarges) as total_payments,
  (ProcedureSummary::avgMedicarePayments * ProcedureSummary::numDischarges) as medicare_payments;

FINANCIAL_AND_PROCEDURE_RAW = JOIN PROCEDURE_SUMMARY_TOTALS by provider_zip, FINANCIAL_DATA_TOTALS by zip;
FINANCIAL_AND_PROCEDURE = FOREACH FINANCIAL_AND_PROCEDURE_RAW GENERATE drg_code, drg_name, provider_name, zip, state, num_discharges,
  covered_charges, total_payments, medicare_payments,
  agi_range, num_returns,
  total_agi, total_total_income;

STORE FINANCIAL_AND_PROCEDURE into '/inputs/alih/financial_and_procedure' Using PigStorage(',');
