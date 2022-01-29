package simpledb.execution;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import java.util.*;
/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
    private static final long serialVersionUID = 1L;
    private Predicate pred;
    private final TupleDesc td;
    private OpIterator child;
    //private Iterator<Tuple> it;
    private final List<Tuple> childTups = new ArrayList<>();
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.child = child;
        this.td = child.getTupleDesc();
        pred = p;

    }

    public Predicate getPredicate() {
        return pred;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();                       // 为什么这个要打开
        //it = childTups.iterator();
    }

    public void close() {
        super.close();
        child.close();
        //it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.close();
        child.open();
        //it = childTups.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple nowTuple = child.next();
            if(pred.filter(nowTuple)) {
                childTups.add(nowTuple);
                //it = childTups.iterator();
                return nowTuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if(this.child != children[0])
            this.child = children[0];
    }

}
