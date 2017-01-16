package edu.uchicago.mpcs53013.alih.FinancialTopology;

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

public class FinancialTopology {

	static class GetFinancialDataBolt extends BaseBasicBolt {
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String financialJSON = tuple.getString(0);
			JSONParser parser = new JSONParser();
				
		      try{
		         Object financialData = parser.parse(financialJSON);
		         JSONObject financialObject = (JSONObject)financialData;
		        		    
		        		    
		         try {
						collector.emit(new Values(financialObject.get("agi"),
												  financialObject.get("state"),
												  financialObject.get("range"),
												  financialObject.get("ti")
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
			declarer.declare(new Fields("agi", "state", "range", "ti"));
		}

	}

	static class UpdateFinancialDataBolt extends BaseBasicBolt {
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
			String[] drgs = {"305", "207", "101", "811", "194", "329", "870", "603", "482", "552", "641", "469", "238", "281", "948", "690", "282", "917", "203",
				    "372", "460", "247", "389", "391", "249", "698", "149", "066", "252", "378", "689", "310", "300", "897", "303", "301", "254", "377", "246",
				    "064", "178", "418", "853", "195", "314", "872", "330", "812", "392", "390", "473", "244", "176", "491", "536", "638", "419", "640", "313",
				    "039", "243", "379", "885", "699", "065", "190", "292", "470", "280", "291", "918", "308", "189", "683", "480", "192", "208", "309",
				    "191", "202", "684", "193", "286", "439", "682", "287", "871", "069", "177", "312", "481", "563", "057", "602", "074", "293", "253", "394",
				    "251", "315"};
			
			for (String drg : drgs) {
			try {
				long agi = input.getLongByField("agi");
				String state = input.getStringByField("state");
				String range = input.getStringByField("range");
				long ti = input.getLongByField("ti");
				
				
				String key = drg + range + state;
				
				dataTable = hbaseConnection.getTable(TableName.valueOf("alih_state_totals")); 
				Increment increment = new Increment(Bytes.toBytes(key));
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_returns"), 1);
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_agi"), agi);
				increment.addColumn(Bytes.toBytes("total"), Bytes.toBytes("statewide_total_income"), ti);

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
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "alih-financial-data", "/alih-financial-data","financial_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		 kafkaConfig.zkServers = (List<String>)stormConf.get("storm.zookeeper.servers");
		kafkaConfig.zkRoot = "/alih-financial-data";
//		 kafkaConfig.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("financial-event", kafkaSpout, 1);
		builder.setBolt("finance-data", new GetFinancialDataBolt(), 1).shuffleGrouping("financial-event");
		builder.setBolt("update-finance", new UpdateFinancialDataBolt(), 1).shuffleGrouping("finance-data");
		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 2);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster("localhost", 2181L);
			cluster.submitTopology("alih-financial-topology", conf, builder.createTopology());
		}
	}
}
