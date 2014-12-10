package com.rishav.recommendation.apriori;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalcSupportConfidenceJob extends Configured implements Tool {
	
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setJobName(CalcSupportConfidenceJob.class.getSimpleName());
		
		Scan scan = new Scan();
		scan.setCaching(10000);
		TableMapReduceUtil.initTableMapperJob(
				CreateTables.PROD_PAIR_COUNT_TABLE, scan,
				CalcSupportConfidenceMapper.class,
				ImmutableBytesWritable.class, Writable.class, job);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
				CreateTables.APPRIORI_RESULTS_TABLE);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setNumReduceTasks(0);
		int retCode = job.waitForCompletion(true) ? 0 : 1;
		return retCode;
	}

	public static class CalcSupportConfidenceMapper extends
			TableMapper<ImmutableBytesWritable, Put> {

		// read product support counts and store them in memory
		Map<String, Long> pidSupportCount;
		Long tranCount;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration hbaseConf = HBaseConfiguration.create();
			Scan scan = new Scan();
			scan.setCaching(10000);
			scan.addColumn(CreateTables.SUPPORT_COUNT_CF_AS_BYTES,
					CreateTables.SUPPORT_COUNT_QUALIFIER_AS_BYTES);
			HTable htable = new HTable(hbaseConf, CreateTables.PROD_COUNT_TABLE);
			ResultScanner scanner = htable.getScanner(scan);
			pidSupportCount = new TreeMap<String, Long>();
			for (Result result : scanner) {
				// pid
				String pId = (new Text(result.getRow())).toString();
				// pid count
				Long count = Bytes.toLong(result.getValue(
						CreateTables.SUPPORT_COUNT_CF_AS_BYTES,
						CreateTables.SUPPORT_COUNT_QUALIFIER_AS_BYTES));
				pidSupportCount.put(pId, count);
			}
			htable.close();

			// get transaction count from -D input
			Configuration jobConf = context.getConfiguration();
			try {
				tranCount = Long.parseLong(jobConf.get("transaction.count"));
			} catch (NumberFormatException e) {
				System.err.println("Invalid transaction count: "
						+ jobConf.get("transaction.count"));
				System.exit(1);
			}
			// System.out.println("Printing Support count of products:");
			// printMap(pidSupportCount);
		}

		PIdPair ab;
		Long abCount;
		Long aCount;
		Long bCount;
		Double abSupport;
		Double abConfidence;
		Double abLift;
		Put put;

		@Override
		protected void map(ImmutableBytesWritable key, Result columns,
				Context context) throws IOException, InterruptedException {
			ab = PIdPair.toPidPair(new Text(key.get()).toString());
			abCount = Bytes.toLong(columns.getValue(
					CreateTables.SUPPORT_COUNT_CF_AS_BYTES,
					CreateTables.SUPPORT_COUNT_QUALIFIER_AS_BYTES));
			aCount = pidSupportCount.get(ab.getFirst().toString());
			bCount = pidSupportCount.get(ab.getSecond().toString());

			abSupport = ((double) abCount) / tranCount;
			abConfidence = ((double) abCount) / aCount;
			abLift = ((double) abCount * tranCount) / (aCount * bCount);

			System.out.println(ab.toString() + "\t" + abSupport + "\t"
					+ abConfidence + "\t" + abLift);
			put = new Put(ab.toString().getBytes());
			put.add(CreateTables.APPRIORI_RESULTS_CF_AS_BYTES,
					CreateTables.APPRIORI_SUPPORT_QUALIFIER_AS_BYTES,
					Bytes.toBytes(abSupport));
			put.add(CreateTables.APPRIORI_RESULTS_CF_AS_BYTES,
					CreateTables.APPRIORI_LIFT_QUALIFIER_AS_BYTES,
					Bytes.toBytes(abLift));
			put.add(CreateTables.APPRIORI_RESULTS_CF_AS_BYTES,
					CreateTables.APPRIORI_CONFIDENCE_QUALIFIER_AS_BYTES,
					Bytes.toBytes(abConfidence));
			context.write(new ImmutableBytesWritable(ab.toString().getBytes()),
					put);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CalcSupportConfidenceJob(), args);
		System.exit(exitCode);
	}
}
