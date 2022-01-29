package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import java.util.*;
/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    private static final long serialVersionUID = 1L;
    DbFileIterator seqScanIterator;
    int tableId;
    String tableName;
    String tableAlias;
    TransactionId tid;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        tableId = tableid;
        seqScanIterator = ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).iterator(tid);
        tableName = Database.getCatalog().getTableName(tableid);
        this.tid = tid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() { return tableName; }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() { return tableAlias; }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        seqScanIterator = ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).iterator(tid);
        tableName = Database.getCatalog().getTableName(tableid);
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        seqScanIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc result;
        TupleDesc oldTupleDescription = Database.getCatalog().getDatabaseFile(this.tableId).getTupleDesc();
        Type[] typeAr = new Type[oldTupleDescription.numFields()];
        String[] fileAr = new String[oldTupleDescription.numFields()];
        for(int i = 0; i < typeAr.length; ++i) {
            typeAr[i] = oldTupleDescription.getFieldType(i);
            fileAr[i] = this.tableAlias + "." + oldTupleDescription.getFieldName(i);
        }
        result = new TupleDesc(typeAr, fileAr);
        return result;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return seqScanIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple result = seqScanIterator.next();
        if(result == null) throw new NoSuchElementException("No Such Element Exception");
        return result;
    }

    public void close() {
        seqScanIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        seqScanIterator.rewind();
    }
}
