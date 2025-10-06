package com.distributed.fs;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.println("=================================");
        System.out.println("Distributed Storage System");
        System.out.println("=================================\n");

        StorageNode node = new StorageNode();

        // Run automated tests
        runAutomatedTests(node);

        // Start interactive mode
        runInteractiveMode(node);
    }

    /**
     * Runs automated tests to demonstrate the system
     */
    private static void runAutomatedTests(StorageNode node) {
        System.out.println("\n--- Running Automated Tests ---\n");

        // Test 1: Basic write and read
        System.out.println("TEST 1: Basic Write and Read");
        System.out.println("-----------------------------");
        node.writeFile("test.txt", "Hello World");
        String content = node.readFile("test.txt");
        System.out.println("Read content: " + content);
        System.out.println();

        // Test 2: Multiple writes (versioning)
        System.out.println("TEST 2: Multiple Writes (Versioning)");
        System.out.println("-------------------------------------");
        node.writeFile("version_test.txt", "Version 1 content");
        sleep(100); // Small delay to ensure different timestamps
        node.writeFile("version_test.txt", "Version 2 content");
        sleep(100);
        node.writeFile("version_test.txt", "Version 3 content");
        String latestContent = node.readFile("version_test.txt");
        System.out.println("Latest content: " + latestContent);
        System.out.println();

        // Test 3: Check file status across nodes
        System.out.println("TEST 3: File Status Across Nodes");
        System.out.println("---------------------------------");
        node.getFileStatus("version_test.txt");
        System.out.println();

        // Test 4: Concurrent write simulation
        System.out.println("TEST 4: Concurrent Write Handling");
        System.out.println("----------------------------------");
        node.writeFile("concurrent.txt", "First write");
        node.writeFile("concurrent.txt", "Second write");
        node.handleConcurrentWrite("concurrent.txt");
        System.out.println();

        // Test 5: Multiple files
        System.out.println("TEST 5: Multiple Files");
        System.out.println("----------------------");
        node.writeFile("file1.txt", "Content of file 1");
        node.writeFile("file2.txt", "Content of file 2");
        node.writeFile("file3.txt", "Content of file 3");
        System.out.println("File 1: " + node.readFile("file1.txt"));
        System.out.println("File 2: " + node.readFile("file2.txt"));
        System.out.println("File 3: " + node.readFile("file3.txt"));
        System.out.println();

        System.out.println("--- All Tests Completed ---\n");
    }

    /**
     * Runs interactive console mode for manual testing
     */
    private static void runInteractiveMode(StorageNode node) {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        System.out.println("\n--- Interactive Mode ---");
        System.out.println("Commands: write <filename> <content> | read <filename> | status <filename> | exit\n");

        while (running) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                continue;
            }

            String[] parts = input.split(" ", 3);
            String command = parts[0].toLowerCase();

            switch (command) {
                case "write":
                    if (parts.length < 3) {
                        System.out.println("Usage: write <filename> <content>");
                        break;
                    }
                    String fileName = parts[1];
                    String content = parts[2];
                    boolean success = node.writeFile(fileName, content);
                    System.out.println(success ? "Write successful!" : "Write failed!");
                    break;

                case "read":
                    if (parts.length < 2) {
                        System.out.println("Usage: read <filename>");
                        break;
                    }
                    String fileToRead = parts[1];
                    String readContent = node.readFile(fileToRead);
                    if (readContent != null) {
                        System.out.println("Content: " + readContent);
                    } else {
                        System.out.println("File not found!");
                    }
                    break;

                case "status":
                    if (parts.length < 2) {
                        System.out.println("Usage: status <filename>");
                        break;
                    }
                    String fileToCheck = parts[1];
                    node.getFileStatus(fileToCheck);
                    break;

                case "exit":
                    running = false;
                    System.out.println("Exiting...");
                    break;

                default:
                    System.out.println("Unknown command. Available: write, read, status, exit");
            }

            System.out.println();
        }

        scanner.close();
    }

    /**
     * Helper method to pause execution
     */
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}