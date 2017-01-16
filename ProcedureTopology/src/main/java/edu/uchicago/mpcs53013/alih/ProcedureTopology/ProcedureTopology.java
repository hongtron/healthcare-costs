package edu.uchicago.mpcs53013.alih.ProcedureTopology;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class ProcedureTopology {

	static class GetProcedureDataBolt extends BaseBasicBolt {
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String procedureJSON = tuple.getString(0);
			JSONParser parser = new JSONParser();
				
		      try{
		         Object procedureData = parser.parse(procedureJSON);
		         JSONObject procedureObject = (JSONObject)procedureData;
		        		    
		        		    
		         try {
						collector.emit(new Values(procedureObject.get("state"),
												  procedureObject.get("drg"),
												  procedureObject.get("total_payments"),
												  procedureObject.get("covered_charges"),
												  procedureObject.get("medicare_payments")
												 )
						);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}			


		      }catch(ParseException pe){
				
		         System.out.println("position: " + pe.getPosition());
		         System.out.println(pe);
		      }
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("state", "drg", "total_payments", "covered_charges", "medicare_payments"));
		}

	}

	static class UpdateProcedureDataBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private Connection hbaseConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
			    conf.set("hbase.zookeeper.property.clientPort", "2181");
			    conf.set("hbase.zookeeper.quorum", StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ","));
			    String znParent = (String)stormConf.get("zookeeper.znode.parent");
			    if(znParent == null)
			    	znParent = new String("/hbase");
				conf.set("zookeeper.znode.parent", znParent);
				hbaseConnection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hbaseConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			Table dataTable = null;
			
			for (int i=1; i < 7; i++) {
			try {
				
				String state = input.getStringByField("state");
				String drg = input.getStringByField("drg");
				long total_payments = input.getLongByField("total_payments");
				long covered_charges = input.getLongByField("covered_charges");
				long medicare_payments = input.getLongByField("medicare_payments");
				
				
				String key = drg + Integer.toString(i) + state;
				
				dataTable = hbaseConnection.getTable(TableName.valueOf("alih_state_totals")); 
				Increment increment = new Increment(Bytes.toBytes(key));
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_discharges"), 1);
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_total_payments"), total_payments);
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_covered_charges"), covered_charges);
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_medicare_payments"), medicare_payments);

				dataTable.increment(increment);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if(dataTable != null)
					try {
						dataTable.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Map stormConf = Utils.readStormConfig();
		String zookeepers = StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ",");
		System.out.println(zookeepers);
		ZkHosts zkHosts = new ZkHosts(zookeepers);
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "alih-procedure-data", "/alih-procedure-data","procedure_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		 kafkaConfig.zkServers = (List<String>)stormConf.get("storm.zookeeper.servers");
		kafkaConfig.zkRoot = "/alih-procedure-data";
//		 kafkaConfig.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("procedure-event", kafkaSpout, 1);
		builder.setBolt("procedure-data", new GetProcedureDataBolt(), 1).shuffleGrouping("procedure-event");
		builder.setBolt("update-procedure", new UpdateProcedureDataBolt(), 1).shuffleGrouping("procedure-data");
		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 2);

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
			LocalCluster cluster = new LocalCluster("localhost", 2181L);
			cluster.submitTopology("alih-procedure-topology", conf, builder.createTopology());
		}
	}
}
