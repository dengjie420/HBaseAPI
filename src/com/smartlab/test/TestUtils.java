package com.smartlab.test;

import com.smartlab.utils.HBaseUtils;

public class TestUtils {
	/*
	 * TestUtils : 对各个模块进行测试
	 * 
	 * @Authou : dengjie
	 * 
	 * email : 810371213@qq.com
	 */

	public static void main(String[] args) {

		// HBaseUtils.createTable("User");
//		HBaseUtils.insertData("User");
//		 HBaseUtils.deleteData("User", "smartlab");
		// HBaseUtils.deleteData("User", new
		// String[]{"smartlab411","smartlab"});
		HBaseUtils.getDate("User");

	}
}
