package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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
    TupleDesc td;
    DbFileIterator it;
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
        //this.it = new DbFileIterator();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
    // 写的很奇怪
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
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    // 注意这种写法
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int numsPage = numPages();
            BufferPool bufferPool = Database.getBufferPool();
            int pid = 0;
            private Iterator<Tuple> tupleIterator;
            private HeapPage currPage;
            private boolean isopen = false;

            public void open() throws DbException, TransactionAbortedException {
                isopen = true;
                if(!getPage(pid++))
                    throw new DbException("CAN'T OPEN");
            }

            private boolean getPage(int pid) throws TransactionAbortedException, DbException {
                if(!isopen) throw new DbException("not open ");
                currPage = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                if(currPage == null) return false;
                else
                    tupleIterator = currPage.iterator();
                return true;
            }
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return isopen && ((pid < numsPage) || (pid == (numsPage) &&
                        (tupleIterator != null && tupleIterator.hasNext())));
            }
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!isopen || tupleIterator == null) throw new NoSuchElementException();
                if(!tupleIterator.hasNext()) {
                        getPage(pid++);
                }
                return tupleIterator.next();
            }
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }
            public void close() {
                pid = 0;
                tupleIterator = null;
                currPage = null;
                isopen = false;
            }
        };
    }
}

