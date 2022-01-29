package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private final RandomAccessFile  rf;
    TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        try {
            this.rf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
       //return this.file;
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    // get tableId
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = BufferPool.getPageSize() * pid.getPageNumber();
        byte[] buf = new byte[BufferPool.getPageSize()];
        try{
            RandomAccessFile rdf = new RandomAccessFile(file, "r");
            rdf.seek(offset);
            if(rdf.read(buf) == -1){
                return null;
            }
            rdf.close();
            return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()), buf);
        }catch(Exception ex) {
            throw new IllegalArgumentException();
        }


    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        /*
        FileOutputStream output = new FileOutputStream(this.file, true); // 写入文件的流, append:true 表示写入文件末尾。
        byte[] bytes = page.getPageData();
        output.write(bytes);
        output.close();
         */
        HeapPageId hpid = (HeapPageId) page.getId();
        int pgNo = hpid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pgNo;
        rf.seek(offset);
        rf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pageSize = BufferPool.getPageSize();
        int result =  (int)(file.length()/pageSize);
        return result;
    }
    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if(!t.getTupleDesc().equals(td)) throw new DbException("tupleDescription Mismatch");
        ArrayList<Page> pageList = new ArrayList<>();
        HeapPage nowPage;
        // if BufferPool has page
        for(int i = 0; i < this.numPages(); ++i) {
            nowPage = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            // 当在一个页中招不到对应的空slot,那么可以释放该页上的锁
            if(nowPage.getNumEmptySlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, nowPage.getId());
                continue;
            }

            nowPage.insertTuple(t);
            pageList.add(nowPage);
            return pageList;
        }
        // if the pages in bufferPool is full, add new page
        HeapPageId pageid = new HeapPageId(getId(), this.numPages());
        nowPage = new HeapPage(pageid, HeapPage.createEmptyPageData());
        // 将对应新添加的页写入磁盘中
        nowPage.insertTuple(t);
        pageList.add(nowPage);
        writePage(nowPage);
        return pageList;
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        ArrayList<Page> pageList = new ArrayList<Page>();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                t.getRecordId().getPageId(),Permissions.READ_WRITE);
        if(p == null) throw new DbException("the tuple is not member of this page");
        p.deleteTuple(t);
        pageList.add(p);
        return pageList;
    }
    // see DbFile.java for javadocs
    // 注意这种写法
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapDbFileIterator(this,tid);
    };
}

