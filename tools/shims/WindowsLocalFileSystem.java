package com.geopulse.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * WindowsLocalFileSystem extends Hadoop 3.3.4's RawLocalFileSystem to provide a pure-Java
 * local filesystem implementation on Windows that avoids external dependencies on winutils.exe
 * and native JNI calls in hadoop.dll (e.g. NativeIO$Windows.access0).
 *
 * Background & Technical Diagnosis:
 * ---------------------------------
 * 1. setPermission() and setOwner():
 *    In standard RawLocalFileSystem, these methods invoke Shell.getSetPermissionCommand()
 *    and Shell.getSetOwnerCommand(), which unconditionally require winutils.exe.
 *    Overridden as no-ops because Windows permissions are governed by native NTFS ACLs.
 *
 * 2. listStatus():
 *    In standard RawLocalFileSystem, listStatus() delegates to FileUtil.list().
 *    FileUtil.list() calls FileUtil.canRead(), which on Windows directly invokes the native
 *    JNI method NativeIO$Windows.access0() without checking NativeIO.isAvailable().
 *    When hadoop.dll is absent, this throws java.lang.UnsatisfiedLinkError during directory
 *    listings (such as FileOutputCommitter.commitJob() -> getAllCommittedTaskPaths()).
 *    Overridden to use standard Java File.list(), performing pure-Java error checks
 *    and preserving standard Hadoop FileStatus semantics, hidden files, and exception types.
 *
 * 3. getFileStatus():
 *    In standard RawLocalFileSystem on Windows (where Stat.isAvailable() is false),
 *    getFileStatus() returns DeprecatedRawLocalFileStatus. When permission or owner fields
 *    are queried, it invokes loadPermissionInfoByNonNativeIO(), which executes winutils.exe ls -F.
 *    Overridden to return a clean, fully populated FileStatus using standard java.io.File
 *    and java.nio.file.Files.readAttributes(), preventing any subsequent winutils execution.
 */
public class WindowsLocalFileSystem extends RawLocalFileSystem {

    /**
     * Default constructor required for Hadoop reflection-based filesystem instantiation
     * via fs.file.impl configuration.
     */
    public WindowsLocalFileSystem() {
        super();
    }

    /**
     * Override setPermission to a no-op on Windows.
     *
     * Suppresses winutils.exe chmod execution. File permissions remain governed by
     * native Windows NTFS ACLs.
     */
    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        // No-op: suppress winutils.exe chmod invocation on Windows.
    }

    /**
     * Override setOwner to a no-op on Windows.
     *
     * Suppresses winutils.exe chown execution. Ownership remains governed by the Windows
     * user security context of the running process.
     */
    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        // No-op: suppress winutils.exe chown invocation on Windows.
    }

    /**
     * Override listStatus to avoid native JNI call NativeIO$Windows.access0() via FileUtil.list().
     *
     * Preserves exact Hadoop semantics:
     * - Throws FileNotFoundException if path does not exist.
     * - Returns array containing all children (including hidden files starting with . or _).
     * - Handles transient file deletions during iteration.
     * - Throws AccessDeniedException if directory cannot be read.
     * - Returns single-entry array if path is a file.
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        File localf = pathToFile(f);
        if (!localf.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        if (localf.isDirectory()) {
            String[] names = localf.list();
            if (names == null) {
                if (!localf.exists()) {
                    throw new FileNotFoundException("File " + f + " does not exist");
                }
                if (!localf.canRead()) {
                    throw new AccessDeniedException(localf.toString(), null, "Permission denied");
                }
                throw new IOException("Failed to list directory: " + f);
            }
            FileStatus[] results = new FileStatus[names.length];
            int j = 0;
            for (int i = 0; i < names.length; i++) {
                try {
                    results[j] = getFileStatus(new Path(f, names[i]));
                    j++;
                } catch (FileNotFoundException e) {
                    // File deleted during listing, ignore as in standard Hadoop
                }
            }
            if (j == names.length) {
                return results;
            }
            return Arrays.copyOf(results, j);
        }
        return new FileStatus[] { getFileStatus(f) };
    }

    /**
     * Override getFileStatus to return a clean, fully populated FileStatus.
     *
     * Standard RawLocalFileSystem returns DeprecatedRawLocalFileStatus on Windows, which
     * attempts to execute winutils.exe ls -F upon getPermission() / getOwner() calls.
     * This implementation uses standard Java I/O and java.nio.file attributes to populate
     * all fields eagerly, eliminating all winutils and NativeIO dependencies.
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        File path = pathToFile(f);
        if (!path.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        long length = path.isDirectory() ? 0L : path.length();
        boolean isDir = path.isDirectory();
        int blockReplication = 1;
        long blockSize = getDefaultBlockSize(f);
        long modTime = path.lastModified();
        long accessTime = 0L;
        try {
            BasicFileAttributes attrs = Files.readAttributes(path.toPath(), BasicFileAttributes.class);
            accessTime = attrs.lastAccessTime().toMillis();
        } catch (IOException ignored) {
            // Fallback to 0 if access time cannot be read
        }
        FsPermission permission = isDir ? FsPermission.getDirDefault() : FsPermission.getFileDefault();
        String owner = System.getProperty("user.name", "owner");
        return new FileStatus(
            length,
            isDir,
            blockReplication,
            blockSize,
            modTime,
            accessTime,
            permission,
            owner,
            owner,
            makeQualified(f)
        );
    }
}
