package com.rishav.recommendation.apriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTables {
	public static String TRAN_BASKET_TABLE = "tranBasket";
	public static String TRAN_BASKET_CF = "pId";
	public static byte[] TRAN_BASKET_CF_AS_BYTES = Bytes
			.toBytes(TRAN_BASKET_CF);
	public static byte[] TRAN_BASKET_PID_PRESENT = Bytes.toBytes(true);

	public static String TRAN_BASKET_PIDPAIR_CF = "pp";
	public static byte[] TRAN_BASKET_PIDPAIR_CF_AS_BYTES = Bytes
			.toBytes(TRAN_BASKET_PIDPAIR_CF);

	public static String PROD_PAIR_COUNT_TABLE = "ppCount";
	public static String PROD_COUNT_TABLE = "pCount";
	// these CFs and qualifiers are used across both PROD_PAIR_COUNT_TABLE and
	// PROD_COUNT_TABLE
	public static String SUPPORT_COUNT_CF = "c";
	public static byte[] SUPPORT_COUNT_CF_AS_BYTES = Bytes
			.toBytes(SUPPORT_COUNT_CF);
	public static String SUPPORT_COUNT_QUALIFIER = "ct";
	public static byte[] SUPPORT_COUNT_QUALIFIER_AS_BYTES = Bytes
			.toBytes(SUPPORT_COUNT_QUALIFIER);

	public static String APPRIORI_RESULTS_TABLE = "apprOut";
	public static String APPRIORI_RESULTS_CF = "a";
	public static byte[] APPRIORI_RESULTS_CF_AS_BYTES = Bytes
			.toBytes(APPRIORI_RESULTS_CF);
	public static String APPRIORI_SUPPORT_QUALIFIER = "s";
	public static byte[] APPRIORI_SUPPORT_QUALIFIER_AS_BYTES = Bytes
			.toBytes(APPRIORI_SUPPORT_QUALIFIER);
	public static String APPRIORI_CONFIDENCE_QUALIFIER = "c";
	public static byte[] APPRIORI_CONFIDENCE_QUALIFIER_AS_BYTES = Bytes
			.toBytes(APPRIORI_CONFIDENCE_QUALIFIER);
	public static String APPRIORI_LIFT_QUALIFIER = "l";
	public static byte[] APPRIORI_LIFT_QUALIFIER_AS_BYTES = Bytes
			.toBytes(APPRIORI_LIFT_QUALIFIER);

	// public static String PROD_MATRIX_TABLE = "prodMatrix";
	// public static String PROD_MATRIX_COUNT_CF = "c";
	// public static byte[] PROD_MATRIX_COUNT_CF_AS_BYTES =
	// Bytes.toBytes(PROD_MATRIX_COUNT_CF);
	// public static String PROD_MATRIX_CO_OC_CF = "co";
	// public static byte[] PROD_MATRIX_CO_OC_CF_AS_BYTES =
	// Bytes.toBytes(PROD_MATRIX_CO_OC_CF);

	public static void createTableAndColumn(Configuration conf, String table,
			byte[] columnFamily) throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
		HColumnDescriptor meta = new HColumnDescriptor(columnFamily);
		desc.addFamily(meta);
		if (hbase.tableExists(table)) {
			if (hbase.isTableEnabled(table)) {
				hbase.disableTable(table);
			}
			hbase.deleteTable(table);
		}
		hbase.createTable(desc);
		hbase.close();
		System.out.println("Created HBase table " + table + " with CF "
				+ Bytes.toString(columnFamily));
	}

	public static void addColumn(Configuration conf, String table,
			byte[] columnFamily) throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(conf);
		HColumnDescriptor meta = new HColumnDescriptor(columnFamily);

		if (hbase.tableExists(table)) {
			if (hbase.isTableEnabled(table)) {
				hbase.disableTable(table);
			}
		}
		hbase.addColumn(Bytes.toBytes(table), meta);
		hbase.enableTable(table);
		hbase.close();
		System.out.println("Added CF " + Bytes.toString(columnFamily)
				+ " to HBase table " + table);
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		createTableAndColumn(conf, TRAN_BASKET_TABLE, TRAN_BASKET_CF_AS_BYTES);
		addColumn(conf, TRAN_BASKET_TABLE, TRAN_BASKET_PIDPAIR_CF_AS_BYTES);

		createTableAndColumn(conf, PROD_PAIR_COUNT_TABLE, SUPPORT_COUNT_CF_AS_BYTES);
		
		createTableAndColumn(conf, PROD_COUNT_TABLE, SUPPORT_COUNT_CF_AS_BYTES);
		
		createTableAndColumn(conf, APPRIORI_RESULTS_TABLE, APPRIORI_RESULTS_CF_AS_BYTES);

	}
}
