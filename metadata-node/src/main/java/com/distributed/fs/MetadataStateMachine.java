package com.distributed.fs;

import java.util.HashMap;
import java.util.Map;

public class MetadataStateMachine {
    private final Map<String, String> map = new HashMap<>();

    // apply command like "PUT key value" or "DELETE key"
    public synchronized void apply(String command) {
        if (command == null) return;
        String[] parts = command.split(" ", 3);
        if (parts.length >= 2) {
            String op = parts[0];
            String key = parts[1];
            if ("PUT".equalsIgnoreCase(op) && parts.length == 3) {
                String value = parts[2];
                map.put(key, value);
                System.out.println("APPLY PUT " + key + " -> " + value);
            } else if ("DELETE".equalsIgnoreCase(op)) {
                map.remove(key);
                System.out.println("APPLY DELETE " + key);
            }
        }
    }

    public synchronized String get(String key) {
        return map.get(key);
    }
}
