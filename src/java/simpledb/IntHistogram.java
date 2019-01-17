package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int bucketsNum;
    private int maxValue;
    private int minValue;
    private int totalNumOfTable;
    private Map<Integer,Integer> valueMap = new HashMap<>();

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
    public IntHistogram(int buckets, int min, int max) {
    	this.bucketsNum = buckets;
        this.maxValue = max;
        this.minValue = min;
        this.totalNumOfTable = 0;
        for(int i=0;i< buckets;i++){
            valueMap.put(i,0);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        this.totalNumOfTable++;
        if(v == this.maxValue){
            int num = this.valueMap.get(this.bucketsNum-1);
            num=num+1;
            this.valueMap.put(this.bucketsNum-1,num);
        }else {
            double tmp = ((double) maxValue - (double) minValue) / (double) this.bucketsNum;
            int index = (int) Math.floor((double) (v - minValue) / (double)tmp);
            int num = this.valueMap.get(index);
            num=num+1;
            this.valueMap.put(index,num);
        }


        /*this.totalNumOfTable++;
        //int tmp = (maxValue-minValue)/this.bucketsNum;
    	for(int i=0;i<this.bucketsNum;i++){
    	    int left = this.minValue+i*tmp;
            int right = left+tmp;
            if(v >=left && v<right){
                int num = this.valueMap.get(i);
                this.valueMap.put(i,num++);
                break;
            }

        }*/
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
        if(this.totalNumOfTable <= 0){
            return -1;
        }
        int index = 0;
        double b_left = 0;
        double b_right = 0;
        double tmp = ((double) maxValue - (double) minValue) / (double) this.bucketsNum;
        boolean isFound = false;
        for(int i=0;i<this.bucketsNum;i++){
            double left = this.minValue+(double)i*tmp;
            double right = left+(double) tmp;
            if(v >=left && v<right){
                index = i;
                b_left = left;
                b_right = right;
                isFound = true;
                break;
            }
            if(v == this.maxValue){
                index = this.bucketsNum-1;
                b_left = this.maxValue-(double)tmp;
                b_right = this.maxValue;
                isFound = true;
                break;
            }
        }
        int size = this.valueMap.get(index);

    	if(op == Predicate.Op.EQUALS){
            if(!isFound){
                return 0d;
            }
            return ((double) size/(double) tmp)/((double) this.totalNumOfTable);
        }else if(op == Predicate.Op.GREATER_THAN ){
            if(!isFound){
                return this.minValue>v ? 1d:0d;
            }
            double b_f = (double) size/(double) this.totalNumOfTable;
            double b_part = ((double) b_right-(double) v)/(double) tmp;
            int sum = 0;
            for(int k = index+1;k<this.bucketsNum;k++){
                sum += this.valueMap.get(k);
            }
            return (b_f * b_part+(double) sum/(double) this.totalNumOfTable);

        }else if(op == Predicate.Op.LESS_THAN){
            if(!isFound){
                return this.maxValue<v ? 1d:0d;
            }
            double b_f = (double) size/(double) this.totalNumOfTable;
            double b_part = ((double) v-(double) b_left)/(double) tmp;
            int sum = 0;
            for(int k = 0;k<index;k++){
                sum += this.valueMap.get(k);
            }
            return (b_f * b_part+(double) sum/(double) this.totalNumOfTable);

        }else if(op == Predicate.Op.LESS_THAN_OR_EQ){
            if(!isFound){
                return this.maxValue<v ? 1d:0d;
            }
            return estimateSelectivity(Predicate.Op.LESS_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);

        }else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            if(!isFound){
                return this.minValue>v ? 1d:0d;
            }
            return estimateSelectivity(Predicate.Op.GREATER_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);
        }else if(op == Predicate.Op.NOT_EQUALS){
            return (1d-estimateSelectivity(Predicate.Op.EQUALS,v));
        }else {
            return -1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
