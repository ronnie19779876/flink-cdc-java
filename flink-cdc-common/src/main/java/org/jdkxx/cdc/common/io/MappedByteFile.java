package org.jdkxx.cdc.common.io;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@Slf4j
public class MappedByteFile<T> implements ObjectBinaryFile<T> {
    private static final int EOF = -1;
    private static final int SERIALIZE_BUFFER_SIZE = 5 * 1024 * 1024; //5MB the size of the collection.
    private final long size;
    private final String prefix;
    private final Path directory;
    @Getter
    private final MappedBytePointer pointer;
    private final Map<Integer, FileChannel> channels = new ConcurrentHashMap<>(5);
    private MappedByteBuffer reader;
    private MappedByteBuffer writer;
    private final Object lock = new Object();

    MappedByteFile(String prefix, Path directory, MappedBytePointer pointer, long size) {
        this.prefix = prefix;
        this.directory = directory;
        this.pointer = pointer;
        this.size = size;
    }

    public void open(OpenOption... options) throws IOException {
        Path path = this.pointer.write().open(this.directory, this.prefix);
        OpenOption[] opts = options != null ? options : new OpenOption[]{StandardOpenOption.READ,
                StandardOpenOption.WRITE};
        FileChannel channel = FileChannel.open(path, opts);
        this.channels.put(this.pointer.write().getIndex(), channel);
        this.writer = channel.map(READ_WRITE, 0, this.size);

        if (!this.channels.containsKey(this.pointer.read().getIndex())) {
            path = this.pointer.read().open(this.directory, this.prefix, false);
            if (!Files.exists(path)) {
                throw new FileNotFoundException(path.toString());
            }
            channel = FileChannel.open(path, opts);
            this.channels.put(this.pointer.read().getIndex(), channel);
        }
        this.reader = channel.map(READ_ONLY, 0, this.size);
    }

    @Override
    public long length() {
        return this.writer.position();
    }

    @Override
    public long position() {
        return this.reader.position();
    }

    @Override
    public byte[] readBytes() throws Exception {
        int size;
        synchronized (lock) {
            this.reader.position((int) this.pointer.reads());
            size = this.reader.getInt();
        }
        if (size == 0) {
            return null;
        }

        if (size != EOF) {
            byte[] bytes = new byte[size];
            this.reader.get(bytes);
            this.pointer.reads(this.reader.position());
            log.debug("read data from MappedBuffer, position:{}, path:{}", this.pointer.reads(),
                    this.pointer.read().path(this.directory, this.prefix));
            return bytes;
        } else {
            readToNext();
        }
        return null;
    }

    @Override
    public int writeBytes(byte[] bytes) throws Exception {
        this.writer.position((int) this.pointer.writes());
        //check current file has enough space for accommodating current bytes.
        // If it has not enough space anymore, we need to write EOF mark into current file and write current bytes
        // into next empty new file.
        if (!hasEnoughSpace(bytes)) {
            this.writer.putInt(EOF);
            writeToNext();
        }

        synchronized (lock) {
            this.writer.putInt(bytes.length);
            this.writer.put(bytes);
            this.pointer.writes(this.writer.position());
        }

        log.debug("write data into MappedBuffer, position:{}, bytes:{}, path:{}",
                this.pointer.writes(),
                bytes.length,
                this.pointer.write().path(this.directory, this.prefix));

        return bytes.length;
    }

    @Override
    public void close() throws Exception {
        if (!channels.isEmpty()) {
            writer.force();
            channels.forEach((key, value) -> {
                try {
                    value.close();
                } catch (IOException e) {
                    log.error("closing channel error, index:{}", key, e);
                }
            });
        }
    }

    public boolean hasEnoughSpace(byte[] bytes) {
        return this.writer.limit() - this.pointer.writes() >= bytes.length + 2 * Integer.BYTES;
    }

    private void writeToNext() throws Exception {
        int index = this.pointer.write().getIndex() + 1;
        Path path = this.pointer.write().open(this.directory, this.prefix, index, true);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.writer = channel.map(READ_WRITE, 0, this.size);
        this.writer.position(0);
        this.channels.put(index, channel);
        this.pointer.write().toNext();
    }

    private void readToNext() throws Exception {
        int index = this.pointer.read().getIndex() + 1;
        if (this.channels.containsKey(index)) {
            this.reader = this.channels.get(index).map(READ_ONLY, 0, this.size);
            FileChannel channel = this.channels.remove(this.pointer.read().getIndex());
            if (channel != null) {
                channel.close();
            }
            this.pointer.readToNext(this.directory, this.prefix);
        }
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private static final long DEFAULT_BUFFER_SIZE = 0x7FFFFFFFL; //2GB
        private static final String DEFAULT_PREFIX = "_temp_log";
        private String prefix = DEFAULT_PREFIX;
        private long size = DEFAULT_BUFFER_SIZE;
        private MappedBytePointer pointer;
        private Path directory;

        public Builder<T> pointer(MappedBytePointer pointer) {
            this.pointer = pointer;
            return this;
        }

        public Builder<T> baseDir(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder<T> prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder<T> size(long size) {
            this.size = size;
            return this;
        }

        public MappedByteFile<T> build() {
            if (pointer == null) {
                pointer = new MappedBytePointer();
            }
            return new MappedByteFile<>(this.prefix, this.directory, this.pointer, this.size);
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean running = new AtomicBoolean(true);

        Path path = Paths.get("/Users/ronnie/Downloads");
        MappedBytePointer pointer = MappedBytePointer.builder()
                .nums(2)
                //.read(6710496L, 3)
                //.write(6710520L, 3)
                //.offset(1)
                .build();

        Builder<Person> builder = MappedByteFile.builder();
        MappedByteFile<Person> file = builder
                .baseDir(path)
                //.size(50L * 1024 * 1024)
                .pointer(pointer)
                .build();
        file.open(StandardOpenOption.READ, StandardOpenOption.WRITE);


        ExecutorService service = Executors.newFixedThreadPool(10);
        service.submit(() -> {
            while (running.get()) {
                /*
                Person person = Person.builder()
                        .id(count.incrementAndGet())
                        .name("jack")
                        .age(18)
                        .address("lakala")
                        .build();
                 */
                byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
                try {
                    int size = file.writeBytes(bytes);
                    log.debug("write size:{}, position:{}", size, file.length());
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
                        log.debug("read bytes, position:{}, size:{}", file.position(), bytes.length);
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
                log.info("close file, pointer:{}", file.getPointer());
            } catch (Exception e) {
                log.error("close error", e);
            }
        }));
    }

    @Data
    @lombok.Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class Person {
        long id;
        String name;
        int age;
        String address;
    }
}
