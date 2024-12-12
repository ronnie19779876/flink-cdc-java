package org.jdkxx.cdc.common.io;

public interface ObjectBinaryFile<T> extends AutoCloseable {
    long length();

    long position();

    byte[] readBytes() throws Exception;

    int writeBytes(byte[] bytes) throws Exception;
}
