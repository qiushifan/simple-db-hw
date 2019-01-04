package simpledb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private Map<Field,Tuple> tuples;
    private TupleDesc tupleDesc;
    private Map<Field,List<String>> groupByMap;
    private List<String> valueSetForNoGroupByColumn;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(this.gbfield == Aggregator.NO_GROUPING){
            String aValue = ((StringField) tup.getField(this.afield)).getValue();
            this.valueSetForNoGroupByColumn.add(aValue);
            if (this.what == Op.COUNT) {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(valueSetForNoGroupByColumn.size()));
                this.tuples.put(new IntField(1), tuple);
            }
        }else {
            Field groupByValue = tup.getField(this.gbfield);
            String aValue = ((StringField) tup.getField(this.afield)).getValue();
            if (this.groupByMap.get(groupByValue) == null) {
                List<String> tmp = new ArrayList<>();
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
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleIterator tupleIterator = new TupleIterator(this.tupleDesc,this.tuples.values());
        return  tupleIterator;
    }

}
