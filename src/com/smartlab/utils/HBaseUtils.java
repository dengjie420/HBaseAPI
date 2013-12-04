package com.smartlab.utils;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {

	/*
	 * HBaseUtils : 对hbase数据库CURD的封装
	 * 
	 * @Author : dengjie
	 * 
	 * email : 810371213@qq.com
	 */

	private static Configuration conf;
	private static HBaseAdmin hAdmin;
	private static HTable tbl;
	static {
		conf = HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.property.clientPort", "2888");
		conf.set("hbase.zookeeper.quorum", "192.168.1.1");
		// conf.set("master", "192.168.1.3:60010");
	}

	// 创建表
	public static void createTable(String tableName) {
		System.out.println("start create table ...");
		try {
			hAdmin = new HBaseAdmin(conf);
			if (hAdmin.tableExists(tableName)) {// 若创建的表存在先删除再创建
				hAdmin.disableTable(tableName);// 操作表之前要对表先离线
				hAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,delete ...");
			}
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			hTableDescriptor.addFamily(new HColumnDescriptor("col1"));
			hTableDescriptor.addFamily(new HColumnDescriptor("col2"));
			hTableDescriptor.addFamily(new HColumnDescriptor("col3"));
			hAdmin.createTable(hTableDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("end create table ...");
	}

	// 插入数据
	public static void insertData(String tableName) {
		System.out.println("start insert data ...");
		try {
			tbl = new HTable(conf, tableName);
			Put put = new Put("smartlab411".getBytes());// 行键唯一
			put.add("col1".getBytes(), null, "aaa".getBytes());// 第1列
			put.add("col2".getBytes(), null, "bbb".getBytes());// 第2列
			put.add("col3".getBytes(), null, "ccc".getBytes());// 第3列
			tbl.put(put);
			tbl.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("success!!! end insert data ...");
	}

	// 删除一条数据（单个行键删除）
	public static void deleteData(String tableName, String rowKey) {
		try {
			tbl = new HTable(conf, tableName);
			Delete del = new Delete(rowKey.getBytes());
			tbl.delete(del);
			tbl.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("delete finish!!!");
	}

	// 删除多条数据（多个行键删除）
	public static void deleteData(String tableName, String[] rowKey) {
		try {
			tbl = new HTable(conf, tableName);
			ArrayList<Delete> alist = new ArrayList<Delete>();
			for (String key : rowKey) {
				Delete del = new Delete(key.getBytes());
				alist.add(del);
			}
			tbl.delete(alist);
			tbl.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("delete alist finish!!!");
	}

	// 得到一条数据（单个行键）
	public static void getDate(String tableName, String rowKey) {
		try {
			tbl = new HTable(conf, tableName);
			Get get = new Get(rowKey.getBytes());
			Result rs = tbl.get(get);
			for (KeyValue k : rs.raw()) {
				System.out.println("rowKey: "
						+ Bytes.toStringBinary(k.getRow()));
				System.out.println("timespan: " + k.getTimestamp());
				System.out.println("colFamily: "
						+ Bytes.toStringBinary(k.getFamily()));
				System.out.println("col: "
						+ Bytes.toStringBinary(k.getQualifier()));
				System.out.println("val: " + Bytes.toString(k.getValue()));
			}
			tbl.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 扫描指定数据或是所有数据
	public static void getDate(String tableName) {
		try {
			tbl = new HTable(conf, tableName);
			// Scan scan = new
			// Scan(Bytes.toBytes("startRowID"),Bytes.toBytes("endRowID"));
			Scan scan = new Scan();
			ResultScanner scanner = tbl.getScanner(scan);
			for (Result rs : scanner) {
				System.out.println("************************");
				for (KeyValue k : rs.raw()) {
					System.out.println("rowKey: "
							+ Bytes.toStringBinary(k.getRow()));
					System.out.println("timespan: " + k.getTimestamp());
					System.out.println("colFamily: "
							+ Bytes.toStringBinary(k.getFamily()));
					System.out.println("col: "
							+ Bytes.toStringBinary(k.getQualifier()));
					System.out.println("val: " + Bytes.toString(k.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
