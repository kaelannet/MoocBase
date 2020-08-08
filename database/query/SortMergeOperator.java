package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        // potentially remove and create new comparator
        private LeftRightRecordComparator comparator;

        private SortOperator leftSorter;
        private SortOperator rightSorter;

        private String leftTableName;
        private String rightTableName;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            this.leftSorter = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
            this.rightSorter = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
            this.leftTableName = leftSorter.sort();
            this.rightTableName = rightSorter.sort();
            this.leftIterator = getRecordIterator(leftTableName);
            this.rightIterator =  getRecordIterator(rightTableName);
            this.comparator = new LeftRightRecordComparator();

            fetchNextLeftRecord();
            fetchNextRightRecord();

            this.marked = false;

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

        }

        public void fetchNextLeftRecord() {
            if (!leftIterator.hasNext()) {
                leftRecord = null;
            }
            leftRecord = leftIterator.next();
        }

        public void resetRightRecord() {
            this.rightIterator.reset();
            assert(rightIterator.hasNext());
            rightRecord = rightIterator.next();
        }

        public void fetchNextRightRecord() {
            if (!rightIterator.hasNext()) {
                rightRecord = null;
            }
            rightRecord = rightIterator.next();
        }

        public void fetchNextRecord() {

            while(true) {
                if (leftRecord == null) {
                    nextRecord = null;
                    throw new  NoSuchElementException("No more records to yield");
                }
                if (rightRecord != null) {
                    // R > L so advance L, if Marked then set reset
                    if (comparator.compare(rightRecord, leftRecord) > 0) {
                        fetchNextLeftRecord();
                        if (marked) {
                            marked = false;
                            resetRightRecord();
                        }
                    }
                    // R = L so mark R and advance rightIterator if it hasNext();
                    // If already marked, advance rightIterator, and update R to next record
                    else if (comparator.compare(rightRecord, leftRecord) == 0) {
                        nextRecord = joinRecords(rightRecord, leftRecord);
                        if (!marked) {
                            rightIterator.markPrev();
                            marked = true;
                        }
                        if  (rightIterator.hasNext()) {
                            rightRecord = rightIterator.next();
                        }
                        // If rightIterator doesn't have next, check LeftIterator
                        // If it hasNext(), call resetRightRecord()
                        else if (leftIterator.hasNext()) {
                            resetRightRecord();
                            fetchNextLeftRecord();
                        } else {
                            leftRecord = null;
                        }
                        break;
                    }
                    // R < L so advance R
                    else if (comparator.compare(rightRecord, leftRecord) < 0) {
                        fetchNextRightRecord();
                    }
                }
            }

        }
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

            /**
             * Checks if there are more record(s) to yield
             *
             * @return true if this iterator has another record to yield, otherwise false
             */
        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        private class LeftRightRecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }
    }
}
