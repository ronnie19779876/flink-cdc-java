package org.tikv.cdc;

public interface ChangeEventObserver extends AutoCloseable {
    void start(final long version);

    CDCEvent get() throws Exception;

    void onChangeEvent(CDCEvent event);
}
