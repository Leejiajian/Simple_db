package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private double width_bucket;// 因为可能width是小数,所以应声明为浮点数.
    // value: 直方图的高度，表示在这个区域内tuples的个数
    private ArrayList<Integer> list;
    private final int max, min;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    // buckets: 直方图横坐标的个数
    public IntHistogram(int buckets, int min, int max) {
    	this.ntups = 0;
        this.max = max;
    	this.min = min;
    	list = new ArrayList<>(buckets);
        this.width_bucket = (max - min + 1) / (double)buckets;  // 最后一个边界可能大于max
        // 初始化每个直方, 高度为0
        for(int i = 0; i < buckets; ++i)
            list.add(0);


    }
    public int getIndex(int value) {
        //if(value < min || value > max) throw new NoSuchElementException();
        // when value == max 将其放入最后一个桶中
        if(value == max) return (list.size() - 1);
        return (int)((value - this.min) / this.width_bucket);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v < this.min || v > this.max) throw new NoSuchElementException();
        int index = getIndex(v);
        list.set(index, list.get(index) + 1);
        ++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */

    public double estimateSelectivity(Predicate.Op op, int v) {
        double result = 0;
        if(this.ntups == 0) return 0;
        int index = getIndex(v);
        double b_left = min + (index * width_bucket);       // 当前的左边界
        double b_right = (min + (index+1) * width_bucket);  // 当前的右边界
        // 这里直接算出来的 equal不准,无法通过 TableStatsTest测试
        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if(v < min) return 1;
            if(v > max) return 0;

            double b_f = list.get(index) / (double)this.ntups;
            double b_part = (b_right - v) /this.width_bucket;
            result += b_f * b_part;
            for(int i = index + 1; i < list.size(); ++i) {
                b_f = list.get(i) / (double) this.ntups;
                b_part = 1;
                result += b_f * b_part;
            }
            return result;
        }
        else if(op.equals(Predicate.Op.GREATER_THAN)){
            return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v+1);
        }
        else if(op.equals(Predicate.Op.LESS_THAN))
            return (1 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ,v));
        else if(op.equals(Predicate.Op.LESS_THAN_OR_EQ))
            return (1 - estimateSelectivity(Predicate.Op.GREATER_THAN,v));
        else if(op.equals(Predicate.Op.EQUALS))
            return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.GREATER_THAN,v);
        else
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    // 用 Op.GREATER_THAN_OR_EQ 来衡量效率
    public double avgSelectivity()
    {
        double sum = 0.0;
        for(int i = min; i <= max; ++i) {
            sum += estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, i);
        }
        return sum / (max - min + 1);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String result;
        result  = "max = " + max + " min = " + min + " bucket_size: " + list.size() + " bucket_width: " + width_bucket;
        return result;
    }
}
