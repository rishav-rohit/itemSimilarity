package com.rishav.recommendation.apriori;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class PopulateTranBasketAndProdPairJob extends Configured implements
		Tool {

	final static String SEPARATOR_CONF_KEY = "transaction.separator";
	final static String DEFAULT_SEPARATOR = ",";
	static final byte[] ONE = Bytes.toBytes(1);

	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.printf(
					"Usage: %s [generic options] <input hdfs file>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setJobName(PopulateTranBasketAndProdPairJob.class.getSimpleName());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(PopulateTransactionBasketMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// define output table
		TableMapReduceUtil.initTableReducerJob(CreateTables.TRAN_BASKET_TABLE,
				PopulateTransactionBasketReducer.class, job);
		int retCode = job.waitForCompletion(true) ? 0 : 1;

		return retCode;
	}

	public static class PopulateTransactionBasketMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		// If a custom separator has been used,
		// decode it back from Base64 encoding.
		String separator = null;

		protected void setup(Context context) {

			Configuration conf = context.getConfiguration();
			separator = conf
					.get(PopulateTranBasketAndProdPairJob.SEPARATOR_CONF_KEY);
			if (separator == null) {
				separator = PopulateTranBasketAndProdPairJob.DEFAULT_SEPARATOR;
			} else {
				separator = new String(Base64.decode(separator));
			}
		}

		String line;
		String fields[];
		String tId;
		String pId;

		@Override
		protected void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {
			line = value.toString();
			fields = line.split(separator);
			tId = fields[0];
			pId = fields[1];
			context.write(new Text(tId), new Text(pId));
		}
	}

	public static class PopulateTransactionBasketReducer extends
			TableReducer<Text, Text, Put> {
		Put tranBasketPut;
		List<Text> cpPIds = new ArrayList<Text>();
		List<PIdPair> pIdPairs = new ArrayList<PIdPair>();

		@Override
		public void reduce(Text tId, Iterable<Text> pIds, Context context)
				throws IOException, InterruptedException {
			cpPIds.clear();
			tranBasketPut = new Put(tId.getBytes());

			cpPIds.clear();
			for (Text pId : pIds) {
				cpPIds.add(new Text(pId));
				tranBasketPut.add(CreateTables.TRAN_BASKET_CF_AS_BYTES, pId
						.toString().getBytes(),
						CreateTables.TRAN_BASKET_PID_PRESENT);
			}
			Collections.sort(cpPIds);
			pIdPairs = PIdPair.getAllPairs(cpPIds);

			for (PIdPair pIdPair : pIdPairs) {
				tranBasketPut.add(CreateTables.TRAN_BASKET_PIDPAIR_CF_AS_BYTES,
						pIdPair.toString().getBytes(),
						CreateTables.TRAN_BASKET_PID_PRESENT);
			}
			context.write(null, tranBasketPut);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PopulateTranBasketAndProdPairJob(),	args);
		System.exit(exitCode);
	}
}
