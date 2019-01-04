package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private Map<Field,Tuple> tuples;
    private TupleDesc tupleDesc;
    private Map<Field,List<Integer>> groupByMap;
    private List<Integer> valueSetForNoGroupByColumn;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.tuples = new LinkedHashMap<>();
        if(gbfield == Aggregator.NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            valueSetForNoGroupByColumn = new ArrayList<>();
        }else {
            this.tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
        groupByMap = new LinkedHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(this.gbfield == Aggregator.NO_GROUPING){
            int aValue = ((IntField) tup.getField(this.afield)).getValue();
            this.valueSetForNoGroupByColumn.add(aValue);
            if (this.what == Op.COUNT) {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(valueSetForNoGroupByColumn.size()));
                this.tuples.put(new IntField(1), tuple);
            }else if (this.what == Op.AVG) {
                Tuple tuple = new Tuple(tupleDesc);
                int sum = 0;
                for(int i=0;i<valueSetForNoGroupByColumn.size();i++){
                    sum = sum+valueSetForNoGroupByColumn.get(i);
                }
                int avg = sum/valueSetForNoGroupByColumn.size();
                tuple.setField(0, new IntField(avg));
                this.tuples.put(new IntField(1), tuple);
            }else if (this.what == Op.SUM) {
                Tuple tuple = new Tuple(tupleDesc);
                int sum = 0;
                for(int i=0;i<valueSetForNoGroupByColumn.size();i++){
                    sum = sum+valueSetForNoGroupByColumn.get(i);
                }
                tuple.setField(0, new IntField(sum));
                this.tuples.put(new IntField(1), tuple);
            }else if (this.what == Op.MAX) {
                Tuple tuple = new Tuple(tupleDesc);
                int max = Integer.MIN_VALUE;
                for(int i=0;i<valueSetForNoGroupByColumn.size();i++){
                    if(valueSetForNoGroupByColumn.get(i) > max){
                        max = valueSetForNoGroupByColumn.get(i);
                    }
                }
                tuple.setField(0, new IntField(max));
                this.tuples.put(new IntField(1), tuple);
            }else if (this.what == Op.MIN) {
                Tuple tuple = new Tuple(tupleDesc);
                int min = Integer.MAX_VALUE;
                for(int i=0;i<valueSetForNoGroupByColumn.size();i++){
                    if(valueSetForNoGroupByColumn.get(i) < min){
                        min = valueSetForNoGroupByColumn.get(i);
                    }
                }
                tuple.setField(0, new IntField(min));
                this.tuples.put(new IntField(1), tuple);
            }
        }else {
            Field groupByValue = tup.getField(this.gbfield);
            int aValue = ((IntField) tup.getField(this.afield)).getValue();
            if (this.groupByMap.get(groupByValue) == null) {
                List<Integer> tmp = new ArrayList<>();
                tmp.add(aValue);
                this.groupByMap.put(groupByValue, tmp);
            } else {
                List valueList = this.groupByMap.get(groupByValue);
                valueList.add(aValue);
            }
            List tmp = this.groupByMap.get(groupByValue);
            if (this.what == Op.COUNT) {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, groupByValue);
                tuple.setField(1, new IntField(tmp.size()));
                this.tuples.put(groupByValue, tuple);
            } else if (this.what == Op.AVG) {
                int tmpSum = 0;
                for (int i = 0; i < tmp.size(); i++) {
                    tmpSum = tmpSum + (int) tmp.get(i);
                }
                int avg = tmpSum / tmp.size();
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, groupByValue);
                tuple.setField(1, new IntField(avg));
                this.tuples.put(groupByValue, tuple);
            } else if (this.what == Op.SUM) {
                int tmpSum = 0;
                for (int i = 0; i < tmp.size(); i++) {
                    tmpSum = tmpSum + (int) tmp.get(i);
                }
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, groupByValue);
                tuple.setField(1, new IntField(tmpSum));
                this.tuples.put(groupByValue, tuple);
            } else if (this.what == Op.MAX) {
                int tmpMax = Integer.MIN_VALUE;
                for (int i = 0; i < tmp.size(); i++) {
                    int tmpValue = (int) tmp.get(i);
                    if (tmpValue > tmpMax) {
                        tmpMax = tmpValue;
                    }
                }
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, groupByValue);
                tuple.setField(1, new IntField(tmpMax));
                this.tuples.put(groupByValue, tuple);
            } else if (this.what == Op.MIN) {
                int tmpMin = Integer.MAX_VALUE;
                for (int i = 0; i < tmp.size(); i++) {
                    int tmpValue = (int) tmp.get(i);
                    if (tmpValue < tmpMin) {
                        tmpMin = tmpValue;
                    }
                }
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, groupByValue);
                tuple.setField(1, new IntField(tmpMin));
                this.tuples.put(groupByValue, tuple);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleIterator tupleIterator = new TupleIterator(this.tupleDesc,this.tuples.values());
        return  tupleIterator;
    }

}
