package org.tikv.cdc.reader;

import javax.annotation.Nullable;
import java.util.Iterator;

public interface TiKVReader<T, Split> {

    /** Return the current split of the reader is finished or not. */
    boolean isFinished();

    /**
     * Add to split to read, this should call only the when reader is idle.
     *
     * @param splitToRead
     */
    void submitSplit(Split splitToRead);

    /** Close the reader and releases all resources. */
    void close();

    /**
     * Reads records from MySQL. The method should return null when reaching the end of the split,
     * the empty {@link Iterator} will be returned if the data of split is on pulling.
     */
    @Nullable
    Iterator<T> pollSplitRecords() throws InterruptedException;
}
