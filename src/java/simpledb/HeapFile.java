package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
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
        return file.hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(this.file));
            int pageSize = BufferPool.getPageSize();
            int pageNo = pid.getPageNumber();
            byte[] onePageData = new byte[pageSize];
            in.skip(pageNo * pageSize);
            in.read(onePageData, 0, pageSize);
            Page onePage = new HeapPage((HeapPageId) pid, onePageData);
            return onePage;
        } catch (Exception e) {
            return null;
        } finally {
            // Close the file on success or error
            try {
                if (in != null)
                    in.close();
            } catch (IOException ioe) {
                // Ignore failures closing the file
            }
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
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        int num = (int) Math.ceil((double) this.file.length() / (double) pageSize);
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
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
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    /**
     * Helper class that implements the Java Iterator for tuples on a BTreeFile
     */
    class HeapFileIterator extends AbstractDbFileIterator {

        Iterator<Tuple> it = null;
        HeapPage curp = null;

        TransactionId tid;
        HeapFile f;

        /**
         * Constructor for this iterator
         *
         * @param f   - the BTreeFile containing the tuples
         * @param tid - the transaction id
         */
        public HeapFileIterator(HeapFile f, TransactionId tid) {
            this.f = f;
            this.tid = tid;
        }

        /**
         * Open this iterator by getting an iterator on the first leaf page
         */
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId root = new HeapPageId(f.getId(), 0);
            curp = (HeapPage) Database.getBufferPool().getPage(tid, root, Permissions.READ_ONLY);
            it = curp.iterator();
        }

        /**
         * Read the next tuple either from the current page if it has more tuples or
         * from the next page by following the right sibling pointer.
         *
         * @return the next tuple, or null if none exists
         */
        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            if (it != null && !it.hasNext())
                it = null;

            while (it == null && curp != null) {
                HeapPageId nextp = curp.getNextPage(f.numPages());
                if (nextp == null) {
                    curp = null;
                } else {
                    curp = (HeapPage) Database.getBufferPool().getPage(tid,
                            nextp, Permissions.READ_ONLY);
                    it = curp.iterator();
                    if (!it.hasNext())
                        it = null;
                }
            }

            if (it == null)
                return null;
            return it.next();
        }

        /**
         * rewind this iterator back to the beginning of the tuples
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * close the iterator
         */
        public void close() {
            super.close();
            it = null;
            curp = null;
        }
    }

}

