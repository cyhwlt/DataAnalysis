package com.dataanalysis.resource;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class HelloUDF extends UDF {

	public Text evaluate(final Text s) {
		if (s == null) {
			return null;
		}
		return new Text("Hello:" + s);
	}

	public static void main(String[] args) {
		HelloUDF udf = new HelloUDF();
		Text result = udf.evaluate(new Text("hive"));
		System.out.println(result.toString());
		
	}
}
