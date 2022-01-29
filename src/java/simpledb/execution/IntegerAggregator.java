package simpledb.execution;

import Zql.ZGroupBy;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op aggregationOp;
    private Map<Field, Integer>  groupMap;
    private Map<Field, Integer> countMap;
    private Map<Field, Integer> sumMap;
    private TupleDesc td;
    //private Aggregator aggregator;


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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.aggregationOp = what;
        groupMap = new HashMap<>();
        countMap = new HashMap<>();
        sumMap = new HashMap<>();
        // set tupleDescription
        String[] names;
        Type[] types;
        if(gbfield == NO_GROUPING) {
            names = new String[]{"aggregateVal"};
            types = new Type[]{Type.INT_TYPE};
        }else {
            names = new String[] {"groupValue", "aggregateVal"};
            types = new Type[] {gbfieldtype, Type.INT_TYPE};
        }
        td = new TupleDesc(types, names);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(tup.getField(this.aField).getType() != Type.INT_TYPE) throw new NoSuchElementException();
        IntField aggField = (IntField)tup.getField(this.aField);
        Field groupByField = gbField == NO_GROUPING ? null : tup.getField(this.gbField);
        int newValue = aggField.getValue();
        if(tup.getField(gbField).getType() != (gbFieldType))
            throw new NoSuchElementException();
        switch(this.aggregationOp) {
            case MAX:
                if (!groupMap.containsKey(groupByField)) {
                    groupMap.put(groupByField, newValue);
                }
                else {
                    groupMap.put(groupByField, Math.max(newValue, groupMap.get(groupByField)));
                }
                break;
            case MIN:
                if(!groupMap.containsKey(groupByField)) {
                    groupMap.put(groupByField, newValue);
                }
                else {
                    groupMap.put(groupByField, Math.min(newValue, groupMap.get(groupByField)));
                }
                break;
            case COUNT:
                if(!groupMap.containsKey(groupByField)) {
                    groupMap.put(groupByField, 1);
                }
                else {
                    groupMap.put(groupByField, 1 + groupMap.get(groupByField));
                }
                break;
            case SUM:
                if(!groupMap.containsKey(groupByField)) {
                    groupMap.put(groupByField, newValue);
                }
                else {
                    groupMap.put(groupByField, newValue + groupMap.get(groupByField));
                }
                break;
            case AVG:

                if (!groupMap.containsKey(groupByField)) {
                    countMap.put(groupByField, 1);
                    sumMap.put(groupByField, newValue);
                }
                else {
                   countMap.put(groupByField, 1 + countMap.get(groupByField));
                   sumMap.put(groupByField, newValue + sumMap.get(groupByField));
                }
                groupMap.put(groupByField, (sumMap.get(groupByField).intValue())/countMap.get(groupByField));
                break;
            default:
                throw new IllegalArgumentException("Aggregate not supported!");

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
        List<Tuple> tuples = new ArrayList<>();
        if(gbField == NO_GROUPING) {
            for(int value : groupMap.values()) {
                Tuple nowTuple = new Tuple(td);
                nowTuple.setField(0, new IntField(value));
                tuples.add(nowTuple);
            }
        }
        else {
            for(Map.Entry<Field, Integer> r : groupMap.entrySet()) {
                Tuple nowTuple = new Tuple(td);
                // gbField的类型来构造
                if(gbFieldType == Type.INT_TYPE)
                    nowTuple.setField(0, new IntField(((IntField)r.getKey()).getValue()));
                else
                    // String的长度最多是100
                    nowTuple.setField(0, new StringField(((StringField)r.getKey()).getValue(),100));
                nowTuple.setField(1, new IntField(r.getValue()));
                tuples.add(nowTuple);
            }
        }
        return new TupleIterator(td, tuples);
    }
    public TupleDesc getTupleDesc() {
        return this.td;
    }

}
