package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc = null;
    private RecordId recordId = null;
    private Field[] fields = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        int size = td.numFields();
        Type[] resType = new Type[size];
        String[] resName = new String[size];
        for (int i = 0; i < td.numFields(); i++) {
            resType[i] = td.getFieldType(i);
            resName[i] = td.getFieldName(i);
        }
        tupleDesc = new TupleDesc(resType, resName);
        fields = new Field[tupleDesc.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i >= 0 && i < fields.length) {
            fields[i] = f;
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i >= 0 && i < fields.length) {
            return fields[i];
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        String res = "";
        if (fields.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                sb.append(fields[i].toString()).append("\t");
            }
            sb.deleteCharAt(sb.length() - 1);
            res = sb.toString();
        }
        return res;
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        tupleDesc = null;
        int size = td.numFields();
        Type[] resType = new Type[size];
        String[] resName = new String[size];
        for (int i = 0; i < td.numFields(); i++) {
            resType[i] = td.getFieldType(i);
            resName[i] = td.getFieldName(i);
        }
        tupleDesc = new TupleDesc(resType, resName);
    }
}
