package de.hhu.bsinfo.dxram.log.storage.diskaccess;

import java.io.File;
import java.io.IOException;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;

/**
 * Disk access using a RAW partitions to bypass the file system and the page cache (Linux).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class RawDiskAccess implements DiskAccessInterface {

    @Override
    public long getFileSize(final Object p_log) {
        return JNIFileRaw.length((Integer) p_log);
    }

    @Override
    public Object createLog(final File p_file, final long p_logSize) throws IOException {
        String fileName = p_file.getName();
        int fileID = JNIFileRaw.open(fileName, p_logSize);
        // Check for error in native Code
        if (fileID < 0) {
            throw new IOException("JNI error: Cannot create or open log file");
        }

        return fileID;
    }

    @Override
    public Object openLog(final File p_file) {
        return JNIFileRaw.open(p_file.getName(), 0);
    }

    @Override
    public void renameLog(File p_file, File p_newFile) throws IOException {
        // TODO
    }

    @Override
    public void closeLog(final Object p_log) throws IOException {
        if (p_log != null) {
            if (JNIFileRaw.close((Integer) p_log) < 0) {
                throw new IOException("Error Closing the log");
            }
        }
    }

    @Override
    public void closeAndRemoveLog(final Object p_log, final File p_file) throws IOException {
        if (JNIFileRaw.close((Integer) p_log) < 0) {
            throw new IOException("Error Closing the log");
        }

        JNIFileRaw.delete((Integer) p_log);
    }

    @Override
    public void read(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_length,
            final long p_readPos) throws IOException {
        if (JNIFileRaw.read((Integer) p_log, p_bufferWrapper.getAddress(), 0, p_length, p_readPos) < 0) {
            throw new IOException("Error reading from log");
        }
    }

    @Override
    public void write(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length, final boolean p_setLength) throws IOException {
        writeToDisk(p_log, p_bufferWrapper, p_bufferOffset, p_writePos, p_length, p_setLength);
    }

    @Override
    public void append(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length) throws IOException {
        writeToDisk(p_log, p_bufferWrapper, p_bufferOffset, p_writePos, p_length, true);
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
    private static void writeToDisk(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper,
            final int p_bufferOffset, final long p_writePos, final int p_length, final boolean p_setFileLength)
            throws IOException {
        if (JNIFileRaw
                .write((Integer) p_log, p_bufferWrapper.getAddress(), p_bufferOffset, p_length, p_writePos, (byte) 0,
                        p_setFileLength ? (byte) 1 : (byte) 0) < 0) {
            throw new IOException("Error writing to log");
        }
    }
}
