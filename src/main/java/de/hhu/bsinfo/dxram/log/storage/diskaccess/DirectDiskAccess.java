package de.hhu.bsinfo.dxram.log.storage.diskaccess;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxutils.jni.JNIFileDirect;

/**
 * Disk access using O_DIRECT to bypass the page cache (Linux).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class DirectDiskAccess implements DiskAccessInterface {

    @Override
    public long getFileSize(final Object p_log) {
        return JNIFileDirect.length((Integer) p_log);
    }

    @Override
    public Object createLog(final File p_file, final long p_logSize) throws IOException {
        boolean success = true;

        if (p_file.exists()) {
            success = p_file.delete();
        }

        if (success && !p_file.getParentFile().exists()) {
            // Create folders
            success = p_file.getParentFile().mkdirs();
        }

        if (success) {
            String path = p_file.getCanonicalPath();
            return JNIFileDirect.open(path, 0, p_logSize);
        }

        return null;
    }

    @Override
    public Object openLog(final File p_file) {
        return JNIFileDirect.open(p_file.getPath(), 1, 0);
    }

    @Override
    public void renameLog(File p_file, File p_newFile) throws IOException {
        Files.move(p_file.toPath(), p_newFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void closeLog(final Object p_log) throws IOException {
        if (p_log != null) {
            if (JNIFileDirect.close((Integer) p_log) < 0) {
                throw new IOException("Error Closing the log");
            }
        }
    }

    @Override
    public void closeAndRemoveLog(final Object p_log, final File p_file) throws IOException {
        if (JNIFileDirect.close((Integer) p_log) < 0) {
            throw new IOException("Error Closing the log");
        }

        if (p_file.exists()) {
            if (!p_file.delete()) {
                throw new FileNotFoundException();
            }
        }
    }

    @Override
    public void read(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_length,
            final long p_readPos) throws IOException {
        if (JNIFileDirect.read((Integer) p_log, p_bufferWrapper.getAddress(), 0, p_length, p_readPos) < 0) {
            throw new IOException("Error reading from log");
        }
    }

    @Override
    public void write(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length, final boolean p_setLength) throws IOException {
        writeToFile(p_log, p_bufferWrapper, p_bufferOffset, p_writePos, p_length, p_setLength);
    }

    @Override
    public void append(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length) throws IOException {
        writeToFile(p_log, p_bufferWrapper, p_bufferOffset, p_writePos, p_length, true);
    }

    /**
     * Writes to log.
     *
     * @param p_log
     *         either a RandomAccessFile or an Integer storing the file id (DIR and RAW)
     * @param p_bufferWrapper
     *         the buffer wrapper containing the byte buffer to written
     * @param p_bufferOffset
     *         the offset within the byte buffer
     * @param p_writePos
     *         the log position to write to
     * @param p_length
     *         the number of bytes to write
     * @throws IOException
     *         if the log could not be written
     */
    private static void writeToFile(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper,
            final int p_bufferOffset, final long p_writePos, final int p_length, final boolean p_setFileLength)
            throws IOException {
        if (JNIFileDirect
                .write((Integer) p_log, p_bufferWrapper.getAddress(), p_bufferOffset, p_length, p_writePos, (byte) 0,
                        p_setFileLength ? (byte) 1 : (byte) 0) < 0) {
            throw new IOException("Error writing to log");
        }
    }
}
