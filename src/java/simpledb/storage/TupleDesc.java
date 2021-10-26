package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    private TDItem[] tdItems;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new TDItemIterator();
    }
    private class TDItemIterator implements Iterator<TDItem> {
        private int pos;
        public TDItemIterator() {
            pos = 0;
        }
        public boolean hasNext(){
            return pos < tdItems.length;
        }
        public TDItem next(){
            TDItem returnItem = tdItems[pos];
            ++pos;
            return returnItem;
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdItems = new TDItem[typeAr.length];   // 创建多个域描述
        for(int i = 0; i < typeAr.length; ++i) {
            if(typeAr[i] != null){
                tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
            }
            else{
                tdItems[i] = new TDItem(null, null);
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        tdItems = new TDItem[typeAr.length];
        for(int i = 0; i < typeAr.length; ++i) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {

        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i >= tdItems.length)
            throw new NoSuchElementException("no such Element Exception");
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i < 0 || i >= tdItems.length)
            throw new NoSuchElementException("no such Element Exception");
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name == null) throw new NoSuchElementException("No Such Element Exception");
        for(int i = 0; i < tdItems.length; ++i){
            if(name.equals(tdItems[i].fieldName) )
                return i;
        }
        throw new NoSuchElementException("No Such Element Exception");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int len = 0;
        for(int i = 0; i < tdItems.length; ++i){
            len += tdItems[i].fieldType.getLen();
        }
        return len;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int len1 = td1.tdItems.length;
        int len2 = td2.tdItems.length;
        String[] fieldN = new String[len1+len2];
        Type[] typeArray = new Type[len1+len2];
        int pos = 0;
        while(pos < len1){
            fieldN[pos] = td1.tdItems[pos].fieldName;
            typeArray[pos] = td1.tdItems[pos].fieldType;
            ++pos;
        }
        int cur = 0;
        while(pos < len1 + len2){
            fieldN[pos] = td2.tdItems[cur].fieldName;
            typeArray[pos] = td2.tdItems[cur].fieldType;
            ++pos;
            ++cur;
        }
        TupleDesc result = new TupleDesc(typeArray, fieldN);
        return result;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(!(o instanceof TupleDesc)) return false;
        if(((TupleDesc) o).tdItems.length != tdItems.length) return false;
        TupleDesc nowTupleDesc = (TupleDesc) o;
        for(int i = 0; i < tdItems.length; ++i){
            if(tdItems[i].fieldType != nowTupleDesc.tdItems[i].fieldType ||
                    tdItems[i].fieldName != nowTupleDesc.tdItems[i].fieldName)
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = "";
        for(int i = 0; i < tdItems.length; ++i) {
            if(i != 0) result += ",";
            result += tdItems[i].fieldType + "(" + tdItems[i].fieldName + ")";
        }
        return result;
    }
    // 返回一个模式数组
    public TDItem[] getTdItems(){
        return tdItems;
    }
}
