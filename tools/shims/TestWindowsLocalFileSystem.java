package com.geopulse.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Focused Java integration test verifying WindowsLocalFileSystem operations on Windows
 * without requiring Spark, Sedona, winutils.exe, or hadoop.dll.
 */
public class TestWindowsLocalFileSystem {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting TestWindowsLocalFileSystem verification...");

        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "com.geopulse.hadoop.WindowsLocalFileSystem");

        // 1. Instantiate via Hadoop reflection
        FileSystem fs = FileSystem.get(URI.create("file:///"), conf);
        if (!(fs instanceof WindowsLocalFileSystem)) {
            throw new AssertionError("Expected WindowsLocalFileSystem, got: " + fs.getClass().getName());
        }
        System.out.println("1. Instantiation via Hadoop reflection: PASS");

        Path testDir = new Path("tools/shims/build_test_dir").makeQualified(fs.getUri(), fs.getWorkingDirectory());
        Path testFile = new Path(testDir, "part-00000.csv");
        Path successFile = new Path(testDir, "_SUCCESS");

        // Clean up previous test artifacts if present
        if (fs.exists(testDir)) {
            fs.delete(testDir, true);
        }

        // 2. mkdirs
        boolean mkdirOk = fs.mkdirs(testDir);
        if (!mkdirOk || !fs.exists(testDir)) {
            throw new AssertionError("fs.mkdirs failed for: " + testDir);
        }
        System.out.println("2. fs.mkdirs: PASS");

        // 3. setPermission and setOwner no-ops
        fs.setPermission(testDir, new FsPermission((short) 0755));
        fs.setOwner(testDir, "testuser", "testgroup");
        System.out.println("3. fs.setPermission and fs.setOwner (no-ops): PASS");

        // 4. File creation and write
        byte[] content = "store_id,foot_traffic\n1,420\n2,850\n".getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = fs.create(testFile)) {
            out.write(content);
        }
        try (OutputStream out = fs.create(successFile)) {
            // empty _SUCCESS marker
        }
        System.out.println("4. fs.create and writing: PASS");

        // 5. getFileStatus verification
        FileStatus fileStatus = fs.getFileStatus(testFile);
        if (fileStatus.isDirectory()) {
            throw new AssertionError("File reported as directory: " + testFile);
        }
        if (fileStatus.getLen() != content.length) {
            throw new AssertionError("Expected length " + content.length + ", got: " + fileStatus.getLen());
        }
        if (fileStatus.getPermission() == null) {
            throw new AssertionError("FileStatus permission was null");
        }
        if (fileStatus.getOwner() == null || fileStatus.getOwner().isEmpty()) {
            throw new AssertionError("FileStatus owner was empty");
        }

        FileStatus dirStatus = fs.getFileStatus(testDir);
        if (!dirStatus.isDirectory()) {
            throw new AssertionError("Directory reported as file: " + testDir);
        }

        try {
            fs.getFileStatus(new Path(testDir, "non_existent.csv"));
            throw new AssertionError("Expected FileNotFoundException for non-existent file");
        } catch (FileNotFoundException expected) {
            // Expected
        }
        System.out.println("5. fs.getFileStatus (file, dir, non-existent): PASS");

        // 6. listStatus verification (testing FileUtil.list bypass & NativeIO bypass)
        FileStatus[] listed = fs.listStatus(testDir);
        if (listed == null || listed.length != 2) {
            throw new AssertionError("Expected 2 files in directory listing, got: " + (listed == null ? "null" : listed.length));
        }

        boolean foundCsv = false;
        boolean foundSuccess = false;
        for (FileStatus st : listed) {
            String name = st.getPath().getName();
            if ("part-00000.csv".equals(name)) {
                foundCsv = true;
                if (st.getLen() != content.length) {
                    throw new AssertionError("part-00000.csv length mismatch: " + st.getLen());
                }
            } else if ("_SUCCESS".equals(name)) {
                foundSuccess = true;
            }
        }
        if (!foundCsv || !foundSuccess) {
            throw new AssertionError("Listing did not contain both expected files: foundCsv=" + foundCsv + ", foundSuccess=" + foundSuccess);
        }

        try {
            fs.listStatus(new Path(testDir, "non_existent_subdir"));
            throw new AssertionError("Expected FileNotFoundException for non-existent directory listing");
        } catch (FileNotFoundException expected) {
            // Expected
        }
        System.out.println("6. fs.listStatus (directory listing, hidden files, non-existent): PASS");

        // 7. Cleanup
        boolean deleted = fs.delete(testDir, true);
        if (!deleted || fs.exists(testDir)) {
            throw new AssertionError("Failed to recursively delete test directory: " + testDir);
        }
        System.out.println("7. fs.delete recursive cleanup: PASS");

        System.out.println("=== ALL TestWindowsLocalFileSystem ASSERTIONS PASSED SUCCESSFULLY ===");
    }
}
