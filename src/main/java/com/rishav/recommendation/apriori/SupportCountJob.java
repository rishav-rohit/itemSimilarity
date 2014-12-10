package com.rishav.recommendation.apriori;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SupportCountJob extends Configured implements Tool {

	static final long ONE = new Long(1);
	static final byte[] ONE_AS_BYTES = Bytes.toBytes(ONE);

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setJobName(SupportCountJob.class.getSimpleName());

		Scan scan = new Scan();
		scan.setCaching(1000);
		TableMapReduceUtil.initTableMapperJob(CreateTables.TRAN_BASKET_TABLE,
				scan, CalcSupportCountMapper.class, Text.class,
				LongWritable.class, job);
		job.setCombinerClass(CalcSupportCountCombiner.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setReducerClass(CalcSupportCountReducer.class);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration());
		int retCode = job.waitForCompletion(true) ? 0 : 1;
		Counters counters = job.getCounters();
		System.out.println("TRANSACTION COUNT: "
				+ counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue());
		return retCode;
	}

	public static class CalcSupportCountMapper extends
			TableMapper<Text, LongWritable> {
		List<KeyValue> kvList = null;

		@Override
		public void map(ImmutableBytesWritable rowKey, Result columns,
				Context context) throws IOException, InterruptedException {
			kvList = columns.list();
			for (KeyValue kv : kvList) {
				context.write(new Text(Bytes.toString(kv.getQualifier())),
						new LongWritable(ONE));
				// System.out.println("Mapper: " +
				// Bytes.toString(kv.getQualifier()) + "~~" + ONE);
			}
		}
	}

	public static class CalcSupportCountCombiner extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text pId, Iterable<LongWritable> ones,
				Context context) throws IOException, InterruptedException {
			Long sum = new Long(0);
			for (@SuppressWarnings("unused")
			LongWritable one : ones) {
				sum = sum + 1;
			}
			context.write(pId, new LongWritable(sum));
		}
	}

	public static class CalcSupportCountReducer extends
			Reducer<Text, LongWritable, ImmutableBytesWritable, Put> {

		public void reduce(Text pId, Iterable<LongWritable> partialCounts,
				Context context) throws IOException, InterruptedException {
			Long count = new Long(0);
			Put prodCountPut;
			for (LongWritable partialCount : partialCounts) {
				count = count + partialCount.get();
			}
			prodCountPut = new Put(pId.toString().getBytes());
			prodCountPut.add(CreateTables.SUPPORT_COUNT_CF_AS_BYTES,
					CreateTables.SUPPORT_COUNT_QUALIFIER_AS_BYTES,
					Bytes.toBytes(count));
			// System.out.println("Reducer: " + pId.toString() + "~~" + count);
			// System.out.println("Reducer Pid is: " + pId.toString() +
			// " index of #: " +pId.toString().indexOf("#") );

			if (pId.toString().indexOf("#") > -1) {
				context.write(
						new ImmutableBytesWritable(Bytes
								.toBytes(CreateTables.PROD_PAIR_COUNT_TABLE)),
						prodCountPut);
			} else {
				context.write(
						new ImmutableBytesWritable(Bytes
								.toBytes(CreateTables.PROD_COUNT_TABLE)),
						prodCountPut);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SupportCountJob(), args);
		System.exit(exitCode);
	}
}
