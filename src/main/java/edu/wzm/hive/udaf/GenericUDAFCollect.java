package edu.wzm.hive.udaf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(
	name = "collect",
	value = "_FUNC_(col) - The parameter is a column name. "
		+ "The return value is a set of the column.",
	extended = "Example:\n"
		+ " > SELECT _FUNC_(col) from src;"
)
public class GenericUDAFCollect extends AbstractGenericUDAFResolver {
	private static final Log LOG = LogFactory.getLog(GenericUDAFCollect.class.getName());
	
	public GenericUDAFCollect() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		
		if(parameters.length != 1){
			throw new UDFArgumentTypeException(parameters.length - 1, 
					"Exactly one argument is expected.");
		}
		
		if(parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
			throw new UDFArgumentTypeException(0, 
					"Only primitive type arguments are accepted but "
					+ parameters[0].getTypeName() + " was passed as parameter 1.");
		}
		
		return new GenericUDAFCollectEvaluator();
	}
	
	@SuppressWarnings("deprecation")
	public static class GenericUDAFCollectEvaluator extends GenericUDAFEvaluator{
		
		private PrimitiveObjectInspector inputOI;
		private StandardListObjectInspector internalMergeOI;
		private StandardListObjectInspector loi;
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			
			if(m == Mode.PARTIAL1 || m == Mode.COMPLETE){
				inputOI = (PrimitiveObjectInspector) parameters[0];
				return ObjectInspectorFactory.getStandardListObjectInspector(
						(PrimitiveObjectInspector) ObjectInspectorUtils
		                .getStandardObjectInspector(inputOI));
			}
			else if(m == Mode.PARTIAL2 || m == Mode.FINAL){
				internalMergeOI = (StandardListObjectInspector) parameters[0];
				inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
				loi = ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
				return loi;
			}
			return null;
		}

		static class ArrayAggregationBuffer implements AggregationBuffer{
			List<Object> container;
		}
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
			reset(ret);
			return ret;
		}
		
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((ArrayAggregationBuffer) agg).container = new ArrayList<Object>();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] param)
				throws HiveException {
			Object p = param[0];
			if(p != null){
				putIntoList(p, (ArrayAggregationBuffer)agg);
			}
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
			ArrayList<Object> partialResult = (ArrayList<Object>) this.internalMergeOI.getList(partial);
			for(Object obj : partialResult){
				putIntoList(obj, myAgg);
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
			ArrayList<Object> list = new ArrayList<Object>();
			list.addAll(myAgg.container);
			return list;
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
			ArrayList<Object> list = new ArrayList<Object>();
			list.addAll(myAgg.container);
			return list;
		}
		
		public void putIntoList(Object param, ArrayAggregationBuffer myAgg){
			Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, this.inputOI);
			myAgg.container.add(pCopy);
		}
	}
}

