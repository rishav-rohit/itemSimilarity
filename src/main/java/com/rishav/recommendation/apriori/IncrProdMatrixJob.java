package com.rishav.recommendation.apriori;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IncrProdMatrixJob {
//public class IncrProdMatrixJob extends Configured implements Tool {
//	static final LongWritable ONE = new LongWritable(1);
//
//	public int run(String[] arg0) throws Exception {
//		Scan scan = new Scan("367|106".getBytes(), "367|3449".getBytes());
//		scan.setCaching(1000);
//		Configuration conf = new Configuration();
//		Job job = new Job(conf);
//		job.setJarByClass(getClass());
//		TableMapReduceUtil.initTableMapperJob(CreateTables.TRAN_BASKET_TABLE,
//				scan, IncrProdMatrixMapper.class, PIdPair.class,
//				LongWritable.class, job);
//		// Path outPath = new Path("output");
//		// FileOutputFormat.setOutputPath(job, outPath);
//		// outPath.getFileSystem(conf).delete(outPath, true);
//		TableMapReduceUtil.initTableReducerJob(CreateTables.PROD_MATRIX_TABLE,
//				IncrProdMatrixReducer.class, job);
//
//		return job.waitForCompletion(true) ? 0 : 1;
//	}
//
//	public static class IncrProdMatrixMapper extends
//			TableMapper<PIdPair, LongWritable> {
//		List<Text> pIds = new ArrayList<Text>();
//		List<KeyValue> kvList = null;
//		List<PIdPair> pIdPairs = null;
//
//		@Override
//		public void map(ImmutableBytesWritable rowKey, Result fields,
//				Context context) throws IOException, InterruptedException {
//			kvList = fields.list();
//			pIds.clear();
//			for (KeyValue kv : kvList) {
//				pIds.add(new Text(Bytes.toString(kv.getQualifier())));
//				// System.out.println(Bytes.toString(kv.getQualifier()));
//				// System.out.println(Bytes.toBoolean(kv.getValue()));
//				// System.out.println(kv.toString());
//			}
//			pIdPairs.clear();
//			pIdPairs = PIdPair.getAllPairs(pIds);
//			for (PIdPair pIdPair : pIdPairs) {
//				context.write(pIdPair, ONE);
//			}
//			// System.out.println("RowKey: " + new String(rowKey.get()));
//			// context.write(new Text(rowKey.get()), new LongWritable(1));
//		}
//	}
//
//	public static class IncrProdMatrixReducer extends
//			TableReducer<PIdPair, LongWritable, Put> {
//		Put prodMatrixPut;
//
//		public void reduce(PIdPair pIdPair, Iterable<LongWritable> ones,
//				Context context) throws IOException, InterruptedException {
//			Long count = new Long(0);
//			prodMatrixPut = new Put(pIdPair.getFirst().getBytes());
//			for (LongWritable one : ones) {
//				count = count + one.get();
//			}
//			prodMatrixPut.add(CreateTables.PROD_MATRIX_CO_OC_CF_AS_BYTES,
//					pIdPair.getSecond().getBytes(), Bytes.toBytes(count));
//			context.write(null, prodMatrixPut);
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//		int exitCode = ToolRunner.run(new IncrProdMatrixJob(), args);
//		System.exit(exitCode);
//	}

}