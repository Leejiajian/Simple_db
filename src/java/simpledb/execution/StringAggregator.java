package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbFieldIndex;
    private Type gbFieldType;
    private int aFieldIndex;
    private Op aggOp;
    private Map<Field, Integer> countMap;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
      // gbfield相同的计数聚集起来
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aFieldIndex = afield;
        this.aggOp = what;
        countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.aggOp != Op.COUNT)
            throw new IllegalArgumentException();
        Field gbField = gbFieldIndex == NO_GROUPING ? null : tup.getField(this.gbFieldIndex);
        if (!tup.getField(gbFieldIndex).getType().equals(gbFieldType)) throw new NoSuchElementException();
        if(countMap.containsKey(gbField))
            countMap.put(gbField, 1+countMap.get(gbField));
        else
            countMap.put(gbField, 1);
    }
    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator(){
        List<Tuple> tuples = new ArrayList<>();
        if(gbFieldIndex == NO_GROUPING) {
            String[] names = new String[] {"aggregateValue"};
            Type[] types = new Type[] {Type.INT_TYPE};
            td = new TupleDesc(types, names);
            for(int value : countMap.values()) {
                Tuple nowTuple = new Tuple(td);
                nowTuple.setField(0, new IntField(value));
                tuples.add(nowTuple);
            }
        }
        else {
            String[] names = new String[] {"groupValue", "aggregateValue"};
            Type[] types = new Type[] {gbFieldType, Type.INT_TYPE};
            td = new TupleDesc(types, names);
            for(Map.Entry<Field, Integer> it : countMap.entrySet()) {
                Tuple nowTuple = new Tuple(td);
                if(gbFieldType == Type.INT_TYPE)
                    nowTuple.setField(0, new IntField(((IntField)it.getKey()).getValue()));
                else
                    // String的长度最多是100
                    nowTuple.setField(0, new StringField(((StringField)it.getKey()).getValue(),100));

                nowTuple.setField(1, new IntField(it.getValue()));
                tuples.add(nowTuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
