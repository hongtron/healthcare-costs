package edu.uchicago.mpcs53013.alih.HdfsIngestProcedure;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import edu.uchicago.mpcs53013.alih.HdfsIngestProcedure.ProcedureSummary;

public class SerializeProcedureSummary {
	static TProtocol protocol;
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/home/mpcs53013/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			final Configuration finalConf = new Configuration(conf);
			final FileSystem fs = FileSystem.get(conf);
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			ProcedureSummaryProcessor processor = new ProcedureSummaryProcessor() {

				Writer writer = SequenceFile.createWriter(finalConf,
						SequenceFile.Writer.file(
								new Path("/inputs/thriftProceduresSummary/proceduresSummary")),
						SequenceFile.Writer.keyClass(IntWritable.class),
						SequenceFile.Writer.valueClass(BytesWritable.class),
						SequenceFile.Writer.compression(CompressionType.NONE));


				@Override
				void processProcedureSummary(ProcedureSummary summary) throws IOException {
					try {
						writer.append(new IntWritable(1), new BytesWritable(ser.serialize(summary)));;
					} catch (TException e) {
						throw new IOException(e);
					}
				}
			};
			processor.processDirectory(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
