package com.distributed.fs;

import java.io.IOException;
import java.util.*;

public class StorageNode {
    private static final String[] NODE_PATHS = {
            "storage_Node1/",
            "storage_Node2/",
            "storage_Node3/"
    };

    private static final int QUORUM_SIZE = 2; // Minimum nodes for successful write
    private final Map<String, Integer> fileVersions; // Track latest version per file

    public StorageNode() {
        this.fileVersions = new HashMap<>();
        Utils.log("StorageNode initialized with " + NODE_PATHS.length + " replicas");
    }

    /**
     * Writes a file to all storage nodes with replication and versioning
     */
    public boolean writeFile(String fileName, String content) {
        Utils.log("Writing file: " + fileName);

        // Get next version number
        int version = fileVersions.getOrDefault(fileName, 0) + 1;
        long timestamp = Utils.getCurrentTimestamp();

        int successfulWrites = 0;
        List<String> failedNodes = new ArrayList<>();

        // Attempt to write to all nodes
        for (String nodePath : NODE_PATHS) {
            try {
                String filePath = nodePath + fileName;
                Utils.writeToFile(filePath, content, version, timestamp);
                successfulWrites++;
                Utils.log("  ✓ Successfully wrote to " + nodePath);
            } catch (IOException e) {
                failedNodes.add(nodePath);
                Utils.log("  ✗ Failed to write to " + nodePath + ": " + e.getMessage());
            }
        }

        // Check if quorum was achieved
        if (successfulWrites >= QUORUM_SIZE) {
            fileVersions.put(fileName, version);
            Utils.log("Write successful! (Quorum: " + successfulWrites + "/" + NODE_PATHS.length + ")");
            return true;
        } else {
            Utils.log("Write failed! Only " + successfulWrites + " nodes updated (quorum requires " + QUORUM_SIZE + ")");
            // Rollback: try to delete from nodes that succeeded
            rollbackWrite(fileName, failedNodes);
            return false;
        }
    }

    /**
     * Reads a file from all nodes and returns the latest version
     */
    public String readFile(String fileName) {
        Utils.log("Reading file: " + fileName);

        List<FileVersion> versions = new ArrayList<>();

        // Read from all nodes
        for (String nodePath : NODE_PATHS) {
            try {
                String filePath = nodePath + fileName;
                FileVersion fv = Utils.readFromFile(filePath);

                if (fv != null) {
                    versions.add(fv);
                    Utils.log("  ✓ Read from " + nodePath + " - " + fv);
                }
            } catch (IOException e) {
                Utils.log("  ✗ Failed to read from " + nodePath + ": " + e.getMessage());
            }
        }

        // Return latest version based on conflict resolution
        if (versions.isEmpty()) {
            Utils.log("File not found in any node");
            return null;
        }

        FileVersion latest = resolveConflicts(versions);
        if (latest != null) {
            Utils.log("Returning latest version: " + latest.getVersion());
            return latest.getContent();
        }
        return null;
    }

    /**
     * Resolves conflicts using last-write-wins strategy
     */
    private FileVersion resolveConflicts(List<FileVersion> versions) {
        if (versions.isEmpty()) {
            return null;
        }

        // Sort and get the latest version
        versions.sort(Collections.reverseOrder());
        FileVersion latest = versions.getFirst();

        // Check for conflicts (multiple versions)
        if (versions.size() > 1) {
            Set<Integer> uniqueVersions = new HashSet<>();
            for (FileVersion fv : versions) {
                uniqueVersions.add(fv.getVersion());
            }

            if (uniqueVersions.size() > 1) {
                Utils.log("CONFLICT DETECTED: Multiple versions found");
                Utils.log("  Using last-write-wins strategy (version " + latest.getVersion() + ")");

                // Optionally, repair inconsistent nodes
                repairInconsistentNodes(latest);
            }
        }

        return latest;
    }

    /**
     * Handles concurrent writes by checking version conflicts
     */
    public void handleConcurrentWrite(String fileName) {
        Utils.log("Checking for concurrent write conflicts on: " + fileName);

        List<FileVersion> versions = new ArrayList<>();

        for (String nodePath : NODE_PATHS) {
            try {
                String filePath = nodePath + fileName;
                FileVersion fv = Utils.readFromFile(filePath);
                if (fv != null) {
                    versions.add(fv);
                }
            } catch (IOException e) {
                Utils.log("  Error reading from " + nodePath);
            }
        }

        resolveConflicts(versions);
    }

    /**
     * Repairs inconsistent nodes by updating them to the latest version
     */
    private void repairInconsistentNodes(FileVersion latest) {
        Utils.log("Repairing inconsistent nodes...");

        for (String nodePath : NODE_PATHS) {
            try {
                String filePath = nodePath + "temp.txt"; // Use a temp filename for demo
                FileVersion currentVersion = Utils.readFromFile(filePath);

                if (currentVersion != null && currentVersion.getVersion() < latest.getVersion()) {
                    Utils.writeToFile(filePath, latest.getContent(), latest.getVersion(), latest.getTimestamp());
                    Utils.log("  ✓ Repaired " + nodePath);
                }
            } catch (IOException e) {
                Utils.log("  ✗ Failed to repair " + nodePath);
            }
        }
    }

    /**
     * Rolls back a failed write by deleting from successfully written nodes
     */
    private void rollbackWrite(String fileName, List<String> failedNodes) {
        Utils.log("Rolling back write for: " + fileName);

        for (String nodePath : NODE_PATHS) {
            if (!failedNodes.contains(nodePath)) {
                try {
                    Utils.deleteFile(nodePath + fileName);
                    Utils.log("  ✓ Rolled back " + nodePath);
                } catch (IOException e) {
                    Utils.log("  ✗ Failed to rollback " + nodePath);
                }
            }
        }
    }

    /**
     * Gets the status of all nodes for a given file
     */
    public void getFileStatus(String fileName) {
        Utils.log("File status for: " + fileName);

        for (String nodePath : NODE_PATHS) {
            try {
                String filePath = nodePath + fileName;
                FileVersion fv = Utils.readFromFile(filePath);

                if (fv != null) {
                    Utils.log("  " + nodePath + ": Version " + fv.getVersion() +
                            " (timestamp: " + fv.getTimestamp() + ")");
                } else {
                    Utils.log("  " + nodePath + ": File not found");
                }
            } catch (IOException e) {
                Utils.log("  " + nodePath + ": Error - " + e.getMessage());
            }
        }
    }
}