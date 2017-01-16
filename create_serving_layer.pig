FINANCIAL_AND_PROCEDURE = LOAD '/inputs/alih/financial_and_procedure' USING PigStorage(',')
as (drg:chararray, drg_name:chararray, provider_name:chararray, zip:chararray, state:chararray, num_discharges:double,
  covered_charges:double, total_payments:double, medicare_payments:double,
  agi_range, num_returns, total_agi, total_total_income);

FINANCIAL_AND_PROCEDURE_FILTERED = FILTER FINANCIAL_AND_PROCEDURE by
  drg is not null and agi_range is not null and state is not null
  and num_discharges is not null and covered_charges is not null and total_payments is not null and medicare_payments is not null and
  agi_range is not null and num_returns is not null and total_agi is not null and total_total_income is not null;

FINANCIAL_AND_PROCEDURE_BY_DRG_AGI_RANGE_STATE = GROUP FINANCIAL_AND_PROCEDURE_FILTERED BY (drg, agi_range, state);
CONTEXTUALIZED_PAYMENTS = FOREACH FINANCIAL_AND_PROCEDURE_BY_DRG_AGI_RANGE_STATE
   GENERATE
   CONCAT(group.drg, group.agi_range, group.state) AS bucket,
   (long)SUM($1.num_discharges) as statewide_discharges,
   (long)SUM($1.num_returns) as statewide_returns,
    (long)SUM($1.total_payments) as statewide_total_payments,
    (long)SUM($1.covered_charges) as statewide_covered_charges,
    (long)SUM($1.medicare_payments) as statewide_medicare_payments,
    (long)SUM($1.total_agi) as statewide_agi,
    (long)SUM($1.total_total_income) as statewide_total_income;

STORE CONTEXTUALIZED_PAYMENTS INTO 'hbase://alih_state_totals'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'total:statewide_discharges, total:statewide_returns,
    total:statewide_total_payments, total:statewide_covered_charges, total:statewide_medicare_payments,
    total:statewide_agi, total:statewide_total_income', '-caster=HBaseBinaryConverter'
  );
