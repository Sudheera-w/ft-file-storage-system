package com.distributed.fs;

public class LogEntry {
    public final int term;
    public final String command; // e.g. "PUT key value" or "DELETE key"

    public LogEntry(int term, String command) {
        this.term = term;
        this.command = command;
    }
}
