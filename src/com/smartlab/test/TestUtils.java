package com.smartlab.test;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.smartlab.utils.HBaseUtils;

public class TestUtils {
	/*
	 * TestUtils : 对各个模块进行测试
	 * 
	 * @Authou : dengjie
	 * 
	 * email : 810371213@qq.com
	 */
	private final String tableName = "userinfo";

	public static void main(String[] args) {

		// // HBaseUtils.createTable(tableName,new String[]{"info","grade"});
		// HBaseUtils.insertData(tableName, rowKey, columnFamily, column,
		// value);
		// ;
		// HBaseUtils.deleteData("User", "smartlab");
		// HBaseUtils.deleteData("User", new
		// String[]{"smartlab411","smartlab"});
		// HBaseUtils.getDate("User");

	}

	// 测试创建表
	@Test
	public void testCreateTable() {
		try {
			HBaseUtils.createTable(tableName, new String[] { "info", "grade" });
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	// 测试插入单条数据
	@Test
	public void testSingleData() {
		HBaseUtils.insertData(tableName, "dengjie", "info", "age", "22");
	}
	
	// 测试插入多条数据
	@Test
	public void testMulData(){
		ArrayList<Put> alists = new ArrayList<Put>();
		Put put = new Put(Bytes.toBytes("dengjie"));// 行键唯一
		// 参数分别为：列族，列，值
		put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"),
				Bytes.toBytes("M"));
		alists.add(put);
		put.add(Bytes.toBytes("info"), Bytes.toBytes("ID"),
				Bytes.toBytes("201060921"));
		alists.add(put);
		HBaseUtils.insertData(tableName, alists);
	}
}
