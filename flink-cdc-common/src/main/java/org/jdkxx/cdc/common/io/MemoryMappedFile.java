package org.jdkxx.cdc.common.io;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@Slf4j
public class MemoryMappedFile {
    private static final int EOF = -1;
    private static final long DEFAULT_BUFFER_SIZE = 0x7FFFFFFF; //2GB
    private static final String DEFAULT_PREFIX = "_bin_log";
    private final Path path;
    private final String name;
    private final long size;
    private MappedByteBuffer buffer;
    private MappedByteBuffer readOnlyBuffer;
    private final MappedBytePointer pointer;
    private final FileChannel[] channels = new FileChannel[10];
    private final Object lock = new Object();

    public MemoryMappedFile(Path base, MappedBytePointer pointer, String name, long size) {
        this.path = base;
        this.pointer = pointer;
        this.name = name;
        this.size = size;
    }

    public MemoryMappedFile(Path base, MappedBytePointer pointer) {
        this(base, pointer, DEFAULT_BUFFER_SIZE);
    }

    public MemoryMappedFile(Path base, MappedBytePointer pointer, long size) {
        this(base, pointer, DEFAULT_PREFIX, size);
    }

    public MappedBytePointer position() {
        return pointer;
    }

    public void open(OpenOption... options) throws IOException {
        Path path = this.pointer.write().open(this.path, this.name);
        OpenOption[] opts = options != null ? options : new OpenOption[]{StandardOpenOption.READ,
                StandardOpenOption.WRITE};
        FileChannel channel = FileChannel.open(path, opts);
        this.channels[this.pointer.write().getIndex() % this.channels.length] = channel;
        this.buffer = channel.map(READ_WRITE, 0, this.size);

        if (this.channels[this.pointer.read().getIndex() % this.channels.length] == null) {
            path = this.pointer.read().open(this.path, this.name, false);
            if (!Files.exists(path)) {
                throw new FileNotFoundException(path.toString());
            }
            channel = FileChannel.open(path, opts);
            this.channels[this.pointer.read().getIndex() % this.channels.length] = channel;
            this.readOnlyBuffer = channel.map(READ_ONLY, 0, this.size);
        }
    }

    public byte[] readBytes() throws IOException {
        if (this.pointer.read().getIndex() == this.pointer.write().getIndex()) {
            synchronized (lock) {
                return readBytes(this.buffer);
            }
        } else {
            return readBytes(this.readOnlyBuffer);
        }
    }

    public int writeBytes(byte[] bytes) throws IOException {
        synchronized (lock) {
            this.buffer.position((int) this.pointer.writes());
            //check current file has enough space for accommodating current bytes.
            // If it has not enough space anymore, we need to write EOF mark into current file and write current bytes
            // into next empty new file.
            if (!hasEnoughSpace(bytes)) {
                this.buffer.putInt(EOF);
                writeToNext();
            }

            this.buffer.putInt(bytes.length);
            this.buffer.put(bytes);
            this.pointer.writes(this.buffer.position());
        }

        log.debug("write data into MappedBuffer, position:{}, bytes:{}, path:{}",
                this.pointer.writes(),
                bytes.length,
                this.pointer.write().path(this.path, this.name));

        return bytes.length;
    }

    public void close() throws Exception {
        if (channels.length > 0) {
            buffer.force();
            for (FileChannel channel : channels) {
                if (channel != null) {
                    channel.close();
                }
            }
        }
    }

    private void writeToNext() throws IOException {
        int index = this.pointer.write().getIndex() + 1;
        Path path = this.pointer.write().open(this.path, this.name, index, true);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.buffer = channel.map(READ_WRITE, 0, this.size);
        this.buffer.position(0);
        this.channels[index % this.channels.length] = channel;
        this.pointer.write().toNext();
    }

    private boolean hasEnoughSpace(byte[] bytes) {
        return this.buffer.limit() - this.pointer.writes() >= bytes.length + 2 * Integer.BYTES;
    }

    private byte[] readBytes(MappedByteBuffer buffer) throws IOException {
        if (buffer == null) {
            return null;
        }

        buffer.position((int) this.pointer.reads());
        int length = buffer.getInt();
        if (length == 0) {
            return null;
        }
        if (length != EOF) {
            byte[] bytes = new byte[length];
            this.buffer.get(bytes);
            this.pointer.reads(this.buffer.position());
            log.debug("read data from MappedBuffer, position:{}, path:{}", this.pointer.reads(),
                    this.pointer.read().path(this.path, this.name));
            return bytes;
        } else {
            readToNext();
        }
        return null;
    }

    private void readToNext() throws IOException {
        int index = this.pointer.read().getIndex() + 1;
        if (this.channels[index % this.channels.length] != null) {
            FileChannel channel = this.channels[this.pointer.read().getIndex() % this.channels.length];
            if (channel != null) {
                channel.close();
                this.channels[this.pointer.read().getIndex() % this.channels.length] = null;
            }
            this.pointer.readToNext(this.path, this.name);
            if (this.pointer.read().getIndex() < this.pointer.write().getIndex()) {
                this.readOnlyBuffer = this.channels[index % this.channels.length].map(READ_ONLY, 0, this.size);
            } else {
                this.readOnlyBuffer = null;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicLong readCount = new AtomicLong(0);
        AtomicLong writeCount = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);
        Path path = Paths.get("/Users/ronnie/Downloads");
        MappedBytePointer pointer = MappedBytePointer.builder()
                .nums(2)
                //.read(6710496L, 3)
                //.write(6710520L, 3)
                //.offset(1)
                .build();

        MemoryMappedFile file = new MemoryMappedFile(path, pointer/*, 50L * 1024 * 1024*/);
        file.open(StandardOpenOption.READ, StandardOpenOption.WRITE);

        ExecutorService service = Executors.newFixedThreadPool(10);
        service.submit(() -> {
            while (running.get()) {
                try {
                    byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
                    int size = file.writeBytes(bytes);
                    log.debug("write size:{}, position:{}, count:{}", size, file.position().writes(), writeCount.incrementAndGet());
                } catch (Exception e) {
                    log.error("write error", e);
                }
            }
        });

        service.submit(() -> {
            while (running.get()) {
                try {
                    byte[] bytes = file.readBytes();
                    if (bytes != null) {
                        log.info("read bytes, position:{}, count:{}", file.position().reads(), readCount.incrementAndGet());
                    }
                } catch (Exception e) {
                    log.error("read content error", e);
                    break;
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                running.set(false);
                service.shutdown();
                file.close();
                log.info("close file, pointer:{}", file.position());
            } catch (Exception e) {
                log.error("close error", e);
            }
        }));

    }
}
