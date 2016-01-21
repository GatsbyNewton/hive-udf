package edu.wzm.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
	name = "hello",
	value = "_FUNC_(str) - from the input string"
		+ "returns the value that is \"Hello $str\" ",
	extended = "Example:\n"
		+ " > SELECT _FUNC_(str) FROM src;"
)
public class HelloUDF extends UDF{
	
	public String evaluate(String str){
		try {
			return "Hello " + str;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return "ERROR";
		}
	}
}
