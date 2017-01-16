namespace java edu.uchicago.mpcs53013.alih.HdfsIngestProcedure

struct ProcedureSummary {
	1: required string drgCode;
	2: required string drgName;
	3: required string providerName;
	4: required string zip;
	5: required string state;
	6: required double avgCoveredCharges;
	7: required double avgTotalPayments;
	8: required double avgMedicarePayments;
	9: required i32 numDischarges;
}