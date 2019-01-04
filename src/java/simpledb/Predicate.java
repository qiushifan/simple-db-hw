package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int fieldId;
    private Op op;
    private Field value;

    /**
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against.
     * @param op      operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.fieldId = field;
        this.op = op;
        this.value = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        return this.fieldId;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        return this.op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        return this.value;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        return t.getField(this.fieldId).compare(this.op, this.value);
        /*switch (this.op){
            case EQUALS :
                break;
            case GREATER_THAN :
                break;
            case LESS_THAN :
                break;
            case LESS_THAN_OR_EQ :
                break;
            case GREATER_THAN_OR_EQ :
                break;
            case LIKE :
                break;
            case NOT_EQUALS :
                break;
            default:
        }*/
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        return "f = " + this.fieldId + " ; op = " + this.op.toString() + " ; operand = " + this.value.toString();
    }
}
