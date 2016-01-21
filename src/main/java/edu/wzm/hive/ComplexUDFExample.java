package edu.wzm.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Description(
	name = "contains",
	value = "_FUNC_(list, str) - The first parameter is a list or array of strings."
			+ "The second parameter is a string. It returns true or false.",
	extended = "Example:\n"
			+ " > SELECT _FUNC_(list, str) from src;"
)
class ComplexUDFExample extends GenericUDF {

  ListObjectInspector listOI;
  StringObjectInspector elementsOI;
  StringObjectInspector argOI;

  @Override
  public String getDisplayString(String[] arg0) {
    return "arrayContainsExample()"; // this should probably be better
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
    }
    // 1. Check we received the right object types.
    ObjectInspector a = arguments[0];
    ObjectInspector b = arguments[1];
    if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
    }
    this.listOI = (ListObjectInspector) a;
    this.elementsOI = (StringObjectInspector) this.listOI.getListElementObjectInspector();
    this.argOI = (StringObjectInspector) b;
    
    // 2. Check that the list contains strings
    if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list of strings");
    }
    
    // the return type of our function is a boolean, so we provide the correct object inspector
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
    // get the list and string from the deferred objects using the object inspectors
//    List<String> list = (List<String>) this.listOI.getList(arguments[0].get());
    int elemNum = this.listOI.getListLength(arguments[0].get());
//    LazyListObjectInspector llst = (LazyListObjectInspector) arguments[0].get();
//    List<String> lst = llst.
    
    LazyString larg = (LazyString) arguments[1].get();
    String arg = argOI.getPrimitiveJavaObject(larg);
    
//    System.out.println("Length: =======================================================>>>" + elemNum);
//    System.out.println("arg: =======================================================>>>" + arg);
    // see if our list contains the value we need
    for(int i = 0; i < elemNum; i++) {
    	LazyString lelement = (LazyString) this.listOI.getListElement(arguments[0].get(), i);
    	String element = elementsOI.getPrimitiveJavaObject(lelement);
    	if(arg.equals(element)){
    		return new Boolean(true);
    	}
    }
    return new Boolean(false);
  }
  
}