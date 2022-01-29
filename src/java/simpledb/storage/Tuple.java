package simpledb.storage;
import simpledb.common.Type;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a n
     * ew tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    TupleDesc tupleDescription;  // 元组的模式
    Field[] fields;              // 其中的value
    RecordId id;

    public Tuple(TupleDesc td) {
        int len = td.numFields();
        Type[] fieldType = new Type[len];
        String[] fieldName = new String[len];
        for(int i = 0; i < len; ++i) {
            fieldName[i] = td.getTdItems()[i].fieldName;
            fieldType[i] = td.getTdItems()[i].fieldType;
        }
        tupleDescription = new TupleDesc(fieldType, fieldName); // 初始化元组模式
        fields = new Field[len];

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDescription;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return id;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if(i >= fields.length) return;
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        int len = fields.length;
        String result = "";
        for (int i = 0; i < len; ++i) {
            if(i != 0) result += " ";
            result += tupleDescription.getTdItems()[i].fieldName + fields[i];
        }
        return result;
    }
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return new Iterator<Field>() {
            private int pos = 0;

            @Override
            public boolean hasNext() {
                return fields.length > pos;
            }

            @Override
            public Field next() {
                return fields[pos++];
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDescription = td;
    }
}
