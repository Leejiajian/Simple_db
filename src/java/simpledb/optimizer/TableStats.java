package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    // key:tableName value: TableStats
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000; // IO cost per page



    private int numTuple, numPage;
    private int ioCostPerPage;
    private DbFile dbfile;
    private int tableId;
    private int numField;
    int[] maxArr = new int[numField];
    int[] minArr = new int[numField];
    ConcurrentHashMap<Integer, IntHistogram> intHistogramConcurrentHashMap;
    ConcurrentHashMap<Integer, StringHistogram> stringHistogramConcurrentHashMap;


    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();
        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);          // 初始化这个东西, 要干什么？
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        intHistogramConcurrentHashMap = new ConcurrentHashMap<>();
        stringHistogramConcurrentHashMap = new ConcurrentHashMap<>();
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbfile= Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc td = dbfile.getTupleDesc();
        this.numField  = td.numFields();        // 域的数量
        this.numTuple = 0;                      // 统计表中所有的tuple数量
        this.numPage = ((HeapFile)dbfile).numPages();//表中所有的页数
        // 表示每个域最大最小值的数组
        maxArr = new int[numField];//
        minArr = new int[numField];//
        //将数组初始化
        for(int i = 0; i < numField; ++i) {
            maxArr[i] =  Integer.MIN_VALUE;
            minArr[i] = Integer.MAX_VALUE;
        }
        Type[] types = new Type[numField];
        for(int i = 0; i < types.length; ++i)
            types[i] = td.getFieldType(i);
        SeqScan seqScan = new SeqScan(new TransactionId(), tableid, "");
        // 第一遍扫描得到最大最小值的数组
        try{
            seqScan.open();
            while(seqScan.hasNext()) {
                Tuple nowTuple = seqScan.next();
                this.numTuple += 1;
                // i:the index of field, 遍历这个tuple的每个Field
                for(int i = 0; i < numField; ++i) {
                    if(td.getFieldType(i).equals(Type.STRING_TYPE))
                        continue;
                    // IntField 更新 max 和 min
                    if(td.getFieldType(i).equals(Type.INT_TYPE)){
                        int val = ((IntField)nowTuple.getField(i)).getValue();
                        if(maxArr[i] < val)
                            maxArr[i] = val;
                        if(minArr[i] > val)
                            minArr[i] = val;

                    }
                }
            }
            seqScan.rewind();
        }catch(Exception ex) {
            ex.printStackTrace();
        }
        for(int i = 0; i < this.numField; ++i) {
            if(types[i].equals(Type.INT_TYPE)) {
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minArr[i], maxArr[i]);
                intHistogramConcurrentHashMap.put(i, intHistogram);
            }
            if(types[i].equals(Type.STRING_TYPE)){
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                stringHistogramConcurrentHashMap.put(i, stringHistogram);
            }
        }
        // 将所有的value加入直方图中
        try{
            while(seqScan.hasNext()) {
                Tuple nowTuple = seqScan.next();
                for(int i = 0; i < numField; ++i) {
                    if(types[i].equals(Type.INT_TYPE)) {
                        int val = ((IntField)nowTuple.getField(i)).getValue();
                        intHistogramConcurrentHashMap.get(i).addValue(val);
                    }
                    else if(types[i].equals(Type.STRING_TYPE)) {
                        String strVal = ((StringField)nowTuple.getField(i)).getValue();
                        stringHistogramConcurrentHashMap.get(i).addValue(strVal);
                    }
                }
            }
            seqScan.close();
        }catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.numPage * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        double cardinality = numTuple * selectivityFactor;
        return (int) cardinality;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    //
    public double avgSelectivity(int field, Predicate.Op op) {
        int cnt = 0;
        double sum = 0.0;
        for(int i = minArr[field]; i <= maxArr[field]; ++i) {
            sum += estimateSelectivity(field, op, new IntField(i));
            ++cnt;
        }
        return sum / cnt;

    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // field:域的索引 op:操作
        double result = 0;
        if(constant instanceof IntField) {
            result = intHistogramConcurrentHashMap.get(field).
                    estimateSelectivity(op, ((IntField) constant).getValue());
        }
        else if(constant instanceof StringField){
            result = stringHistogramConcurrentHashMap.
                    get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return result;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuple;
    }

}
