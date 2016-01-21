package edu.wzm.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

@Description(
	name = "explode_name",
	value = "_FUNC_(col) - The parameter is a column name."
		+ " The return value is two strings.",
	extended = "Example:\n"
		+ " > SELECT _FUNC_(col) FROM src;"
		+ " > SELECT _FUNC_(col) AS (name, surname) FROM src;"
		+ " > SELECT adTable.name,adTable.surname"
		+ " > FROM src LATERAL VIEW _FUNC_(col) adTable AS name, surname;"
)
public class ExplodeNameUDTF extends GenericUDTF{

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		
		if(argOIs.length != 1){
			throw new UDFArgumentException("ExplodeStringUDTF takes exactly one argument.");
		}
		if(argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector)argOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
			throw new UDFArgumentTypeException(0, "ExplodeStringUDTF takes a string as a parameter.");
		}
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("name");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("surname");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}
	
	@Override
	public void process(Object[] args) throws HiveException {
		// TODO Auto-generated method stub
		String input = args[0].toString();
		String[] name = input.split(" ");
		forward(name);
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}

}
