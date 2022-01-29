package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.math.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
page * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final ConcurrentHashMap<PageId, Page> pages;
    //private final ReadWriteLock rwLock;
    private int size;
    private int capacity;
    private LockManager lockManager = new LockManager();

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new ConcurrentHashMap<>(numPages);
        //this.rwLock = new ReentrantReadWriteLock();
        size = 0;
        capacity = numPages;
    }
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    // 寻找一个页面需要一个锁，锁可能被其他事务所占有。在缓冲池中寻找页面，若存在返回页面，若不存在应该被添加到缓冲池中并返回，
    // 如果空间不够大，会替换出一个页面，并将该页面加入。tid用来判断该页是否已被改写。// 未实现
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

            int type = perm == Permissions.READ_ONLY ? 0 : 1;
            long st = System.currentTimeMillis();
        while (true) {
            //获取锁，如果获取不到会阻塞
            try {
                if (lockManager.requireLock(pid, tid, type)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - st > 500) {
                transactionComplete(tid, false); // 事务中止前释放该事件所有的锁
                throw new TransactionAbortedException();
            }
        }
        if(!pages.containsKey(pid)) {
            if (pages.size() >= capacity)
                evictPage();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            pages.put(pid, page);

        }
        return pages.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            //如果成功提交，将所有脏页写回磁盘
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //如果提交失败，回滚，将脏页的原页面写回磁盘
            recoverPages(tid);
        }
        lockManager.completeTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * // 应该是之后要完成的内容
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    // 需要理解
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = dbfile.insertTuple(tid, t);
        for(Page page : pageList) {
            // 可能不在bufferPool pages中的页
            pages.put(page.getId(), page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        if(recordId == null) throw new DbException("recordid is null when delete tuple");
        DbFile dbfile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
        List<Page> pageList = dbfile.deleteTuple(tid, t);       // 删除已经将页从bufferPool中修改了,但需要将修改的页写入磁盘中
        for(Page page : pageList) {
            //dbfile.writePage(page);
            page.markDirty(true, tid);
            //pages.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(ConcurrentHashMap.Entry<PageId, Page> it: pages.entrySet()) {
            flushPage(it.getKey());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        pages.remove(pid);
    }
    // 将bufferPool中的页恢复到一个比较原始的状态
    public synchronized void recoverPages(TransactionId tid) {
        for(ConcurrentHashMap.Entry<PageId, Page> it : pages.entrySet()) {
            Page nowPage = it.getValue();
            if(nowPage.isDirty() == tid) {
                int tableid = it.getKey().getTableId();
                DbFile file =(HeapFile)Database.getCatalog().getDatabaseFile(tableid);
                nowPage = file.readPage(it.getKey());
                pages.put(nowPage.getId(), nowPage);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        if(!pages.containsKey(pid)) throw new NoSuchElementException();
        Page page = pages.get(pid);
        if(page == null) throw new NoSuchElementException();
        if(page.isDirty() != null){
            page.markDirty(false, page.isDirty());
            // 将特定的页写入磁盘中
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(ConcurrentHashMap.Entry<PageId, Page> it: pages.entrySet()) {
            Page nowPage = it.getValue();
            if(tid.equals(nowPage.isDirty()))
                flushPage(it.getKey());
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // 每次都驱逐第一个
        boolean flag = false;
        for(ConcurrentHashMap.Entry<PageId, Page> it: pages.entrySet()) {
            if(it.getValue().isDirty() == null) {
                discardPage(it.getKey());
                flag = true;
                break;
            }
        }
        if(!flag) throw new DbException("there are all dirty pages");
    }

}
