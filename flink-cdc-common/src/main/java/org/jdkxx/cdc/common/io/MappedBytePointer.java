package org.jdkxx.cdc.common.io;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

@ToString
@AllArgsConstructor
public class MappedBytePointer implements Serializable {
    private Pointer read;
    private Pointer write;
    private int nums;
    private int offset;

    public MappedBytePointer() {
        this(new Pointer(), new Pointer(), 3, 0);
    }

    public long reads() {
        return read.position;
    }

    public void reads(long position) {
        read.position = position;
    }

    public long writes() {
        return write.position;
    }

    public void writes(long position) {
        write.position = position;
    }

    public Pointer read(){ return read; }
    public Pointer write(){ return write; }

    public void readToNext(Path base, String prefix) throws IOException {
        if (read.index - offset >= nums) {
            String name = String.format(prefix + ".%s",
                    StringUtils.leftPad(String.valueOf(offset), 6, "0"));
            Path path = base.resolve(name);
            if (Files.exists(path)) {
                Files.delete(path);
            }
            offset++;
        }
        read.toNext();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Pointer implements Serializable {
        private long position;
        private int index;

        public Path open(Path base, String prefix, boolean created) throws IOException {
            return open(base, prefix, index, created);
        }

        public Path open(Path base, String prefix) throws IOException {
            return open(base, prefix, true);
        }

        public void toNext() {
            this.position = 0;
            index++;
        }

        public Path path(Path base, String prefix) {
            return path(base, prefix, this.index);
        }

        public Path path(Path base, String prefix, int index) {
            String name = String.format(prefix + ".%s",
                    StringUtils.leftPad(String.valueOf(index), 6, "0"));
            return base.resolve(name);
        }

        public Path open(Path base, String prefix, int index, boolean created) throws IOException {
            Path path = path(base, prefix, index);
            if (!Files.exists(path) && created) {
                Files.createFile(path);
            }
            return path;
        }
    }

    public static class Builder {
        private static final int DEFAULT_NUMS = 3;
        private Pointer read;
        private Pointer write;
        private int nums = DEFAULT_NUMS;
        private int offset;

        public Builder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder nums(int nums) {
            this.nums = nums;
            return this;
        }

        public Builder read(Pointer read) {
            this.read = read;
            return this;
        }

        public Builder write(Pointer write) {
            this.write = write;
            return this;
        }

        public Builder read(long position, int index) {
            return read(new Pointer(position, index));
        }

        public Builder write(long position, int index) {
            return write(new Pointer(position, index));
        }

        public MappedBytePointer build() {
            if (read == null) {
                this.read = new Pointer();
            }
            if (write == null) {
                this.write = new Pointer();
            }
            return new MappedBytePointer(read, write, nums, offset);
        }
    }
}
