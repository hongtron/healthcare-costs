# Medical Costs Relative to Income by State
{description}

## Setup
start_big_data_services.sh
hbase thrift start
create 'state_totals', 'total' (from hbase shell)
python app.py (from within webui folder)
kafka queues: alih-procedure-data, alih-financial-data
topologies: ProcedureTopology, FinancialTopology
