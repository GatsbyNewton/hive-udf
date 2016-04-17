package com.didi.bi.feature;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Created by GatsbyNewton on 2016/3/29.
 */
@Description(
        name = "min_datetime",
        value = "_FUNC_(col) - The parametar is a date of string (eg.1970-01-01 00:00:00)."
                + "The return value is a minimun date.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) FROM src;"
)
public class ExternalMin extends AbstractGenericUDAFResolver{

    public ExternalMin() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {

        if(info.length != 1){
            throw new UDFArgumentTypeException(info.length - 1,
                    "Exactly one argument is expected.");
        }

        if(info[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                    "Only primitive type argument are accept, but"
                            + info[0].getTypeName() + "was passed as a parameter.");
        }

        return new ExternalMinEvaluator();
    }

    @SuppressWarnings("deprecation")
    public static class ExternalMinEvaluator extends GenericUDAFEvaluator{

        private PrimitiveObjectInspector inputOI;
        //        private PrimitiveObjectInspector internalMergeOI;
        private PrimitiveObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            inputOI = (PrimitiveObjectInspector)parameters[0];
            outputOI = (PrimitiveObjectInspector)ObjectInspectorUtils.getStandardObjectInspector(inputOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return outputOI;
        }

        static class MyAgg implements AggregationBuffer{
            String min;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MyAgg agg = new MyAgg();
            reset(agg);
            return agg;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            MyAgg agg = (MyAgg)aggregationBuffer;
            agg.min = null;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            merge(aggregationBuffer, objects[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object obj) throws HiveException {
            String param = obj.toString();
            if(param == null || "".equals(param) || param.equals("9999-99-99 99:99:99") || param.equals("0000-00-00 00:00:00")){
                MyAgg agg = (MyAgg)aggregationBuffer;
                agg.min = "9999-99-99 99:99:99";
            }
            else {
                compare(obj, (MyAgg)aggregationBuffer);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            MyAgg agg = (MyAgg)aggregationBuffer;
            return agg.min;
        }

        public void compare(Object obj, MyAgg agg){
            String param = ObjectInspectorUtils.copyToStandardObject(obj, this.inputOI).toString();
            if(agg.min == null || "".equals(agg.min)){
                agg.min = param;
            }
            else{
                //old version
//                long input = toLong(param);
//                long max = toLong(agg.min);
//                if (max < input) {
//                    agg.min = param;
//                }

                //new version
                if(agg.min.compareTo(param) > 0){
                    agg.min = param;
                }
            }
        }

        public long toLong(String str){
            StringBuffer newStr = new StringBuffer();
            String[] strArr = str.split(" ");
            String[] date = strArr[0].split("-");
            String[] time = strArr[1].split(":");
            newStr.append(date[0]).append(date[1]).append(date[2])
                    .append(time[0]).append(time[1]).append(time[2]);

            return Long.parseLong(newStr.toString());
        }
    }
}
