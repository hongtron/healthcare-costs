# Medical Costs Relative to Income by State
This app compares healthcare costs to income metrics gathered from tax returns.

## Architecture
 - data is ingested into HDFS
    - procedure data is ingested via a java application
    - financial data is ingested via a simple bash script
 - procedure and financial data is joined and ingsted into hbase via pig scripts
 - storm topologies are used to retrieve data from hbase
 - kafka is used for queuing

## Setup
 - start_big_data_services.sh
 - hbase thrift start
 - create 'state_totals', 'total' (from hbase shell)
 - python app.py (from within webui folder)
 - kafka queues: alih-procedure-data, alih-financial-data
 - topologies: ProcedureTopology, FinancialTopology
