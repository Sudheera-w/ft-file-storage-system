package com.distributed.fs;

public class FileVersion implements Comparable<FileVersion> {
    private final String content;
    private final int version;
    private final long timestamp;

    public FileVersion(String content, int version, long timestamp) {
        this.content = content;
        this.version = version;
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public int getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(FileVersion other) {
        // Compare by version first, then by timestamp
        if (this.version != other.version) {
            return Integer.compare(this.version, other.version);
        }
        return Long.compare(this.timestamp, other.timestamp);
    }

    @Override
    public String toString() {
        return String.format("FileVersion{version=%d, timestamp=%d, content='%s'}",
                version, timestamp, content);
    }
}