package simpledb.storage;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockManager {
    public class PageLock {
        public static final int SHARE = 0;
        public static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;
        public PageLock(TransactionId tid, int type) {
            this.tid = tid;
            this.type = type;
        }
        public int getType(){
            return this.type;
        }
        public TransactionId getTid() {
            return this.tid;
        }
        public void setType(int type) {
            this.type = type;
        }
    }
    private ConcurrentMap<PageId, ConcurrentMap<TransactionId, PageLock>> pageLocks;
    public LockManager() {
        pageLocks = new ConcurrentHashMap<>();
    }

    public synchronized boolean requireLock(PageId pid, TransactionId tid, int requireType)
            throws InterruptedException, TransactionAbortedException {
        // thread: 当前线程的名字
        final String thread  = Thread.currentThread().getName();
        // pageLock: 当前页的对应的锁
        final String lockType = requireType==0 ? "share_lock" : "exclusive_lock";
        ConcurrentMap<TransactionId, PageLock> pageLock = pageLocks.get(pid);
        // lock: 提供的事务号在提供的页号当前的锁
        //PageLock lock = pageLock.get(tid);
        // 当前页上没有事务,没有锁
        if(pageLocks.size() == 0 || pageLock == null) {
            PageLock newLock = new PageLock(tid, requireType);
            pageLock = new ConcurrentHashMap<>();
            pageLock.put(tid, newLock);
            pageLocks.put(pid, pageLock);
            System.out.println("thread: "+ thread +  "page: "+ pid + " have no transactionId, lock. " +
                    "tid: " + tid + " acquire " + lockType + " accept. ");
            return true;
        }
        PageLock lock = pageLock.get(tid);
        // 以下就是该页上有锁的情况
        // 1.该页上有这个事务的锁
        if(lock != null) {
            // 所有请求共享锁的请求被包括
            if(requireType == PageLock.SHARE) {
                System.out.println("thread: " + thread + "page: " + pid +
                        " have required transactionId and lock: " + "tid: " + tid +
                        " acquire " + lockType + " accept. ");
                return true;
            }
            // 同时，该页上有许多锁，那么这些锁都是共享锁
            if(pageLock.size() > 1) {
                if(requireType == PageLock.EXCLUSIVE) {
                    System.out.println("thread: " + thread + "page: " + pid + " have many share_lock " +
                            "tid: " + tid + " acquire " + lockType + "abort");

                    completeTransaction(tid);// 放掉该事务占有的所有锁
                    throw new TransactionAbortedException();

                }
            }
            // 只有一个锁，同时就是该事务的共享锁，要求一个独占锁
            // 需要将共享锁升级为独占锁
            if(pageLock.size() == 1 && lock.getType() == PageLock.SHARE) {
                lock.setType(requireType);
                pageLock.put(tid, lock);
                pageLocks.put(pid, pageLock);
                System.out.println("thread: "+ thread + " page: "+ pid +
                        " required transaction has a share_lock " +
                        " tid: " + tid + " upgrade to: " + lockType + " accept.");
                return true;
            }
            if(pageLock.size() == 1 && lock.getType() == PageLock.EXCLUSIVE) {
                System.out.println("thread: " + thread + " page: "  + pid +
                        " required transaction has a exclusive_lock" +
                        "tid: " + tid + lockType + "accept.");
                return true;
            }
        }
        // 该事务在该页上没有锁，但是其他事务在该页上有锁
        if(lock == null) {
            // 那么要知道其他事务在上面到底是什么类型的锁.previousLock用来获得之前锁的类型
            PageLock previousLock = null;
            for(PageLock l : pageLock.values()) {
                previousLock = l;
            }
            // previousLock不可能是null，因为之前已经确定了PageLock是不空的
            // 如果是独占锁那么就只有一个必定是独占的，反之，则为共享锁
            if(previousLock.getType() == PageLock.EXCLUSIVE) {
                // 需要阻塞线程
                System.out.println("thread: " + thread + " page: " + pid +
                        " In this page have a Exclusive_lock, need wait!!" +
                        " tid: " + tid + " acquire " + lockType + " wait!!");
                //wait(10);
                return false;
            }
            // 先前事务的是共享锁
            if(previousLock.getType() == PageLock.SHARE) {
                // 如果要求的是共享锁,则获得该锁
                if(requireType == PageLock.SHARE) {
                    lock = new PageLock(tid, requireType);
                    pageLock.put(tid, lock);
                    pageLocks.put(pid, pageLock);
                    System.out.println("thread: "+ thread + "page: "+ pid +
                            " required transaction has no lock but other transactions have some share_locks" +
                            " tid: " + tid + " acquire: " + lockType + " accept.");
                    return true;
                }
                // 如果要求的是独占锁，则该事务会被阻塞
                if(requireType == PageLock.EXCLUSIVE) {
                    System.out.println("thread: " + thread + " page: " + pid +
                            " required transaction has no lock but other transactions have some share_locks " +
                            " tid: " + tid + " acquire " + lockType + " wait!!");

                    //wait(50);
                    return false;
                }
            }
        }
        // 不可能到这，如果到达这里，说明逻辑出了问题
        System.out.println("------------------------------------some other cases--------------------------");
        return false;
    }

    /**
     * 查看指定页面是否被指定事务锁定
     * @param pid
     * @param tid
     * @return
     */
    public synchronized boolean isHoldLock(PageId pid, TransactionId tid) {
        ConcurrentMap<TransactionId, PageLock> pageLock ;
        // 得到对应的页锁
        pageLock = pageLocks.get(pid);
        if(tid == null) return false;
        // 没有该页或者该页上没有事务
        if(pageLock == null || pageLock.size() == 0)
            pageLocks.remove(pid);
        // 得到对应的锁
        PageLock lock = pageLock.get(tid);
        return lock != null;
    }
    /**
     * 释放指定页面的指定事务加的锁
     * @param pid
     * @param tid
     */
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        // 得到页号对应的事务集合
        final String thread = Thread.currentThread().getName();
        ConcurrentMap<TransactionId, PageLock> pageLock = pageLocks.get(pid);
        if(pageLock == null) return;
        if(pageLock.get(tid) == null) return;
        final String lockType = pageLock.get(tid).getType() == 0 ? "share_lock" : "exclusive_lock";
        pageLock.remove(tid);
        System.out.println("thread: " + thread + "page: " + pid + " tid: " + tid + " release a " + lockType +
                " the lock size is " + pageLock.size());
        if(pageLock.size() == 0) {
            pageLocks.remove(pid);
            System.out.println("thread: " + thread + "page: " + pid + " has no TransactionId The size of pageLock is " +
                    pageLocks.size() );
        }
        this.notifyAll();
    }
    // 什么叫完成了事务？？就是释放了该事务上所有的锁.
    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> pages = pageLocks.keySet();
        for(PageId pageid : pages) {
            if(isHoldLock(pageid, tid))
                releaseLock(pageid, tid);
        }

    }
}
