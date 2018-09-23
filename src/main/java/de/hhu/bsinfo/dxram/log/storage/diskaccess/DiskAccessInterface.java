package de.hhu.bsinfo.dxram.log.storage.diskaccess;

import java.io.File;
import java.io.IOException;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;

/**
 * An interface for disk access.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public interface DiskAccessInterface {

    /**
     * Returns the file size.
     *
     * @param p_log
     *         either a RandomAccessFile or an Integer storing the file id (DIR and RAW)
     * @return the file size
     * @throws IOException
     *         if the file could not be accessed
     */
    long getFileSize(final Object p_log) throws IOException;

    /**
     * Creates a log.
     *
     * @param p_file
     *         the file containing the path
     * @param p_logSize
     *         the log size
     * @return the log as either a RandomAccessFile or an Integer (file id)
     * @throws IOException
     *         if the log could not be created
     */
    Object createLog(final File p_file, final long p_logSize) throws IOException;

    /**
     * Opens a log from file.
     *
     * @param p_file
     *         the file containing the path
     * @return the log as either a RandomAccessFile or an Integer (file id)
     * @throws IOException
     *         if the log could not be opened
     */
    Object openLog(final File p_file) throws IOException;

    /**
     * Renames a log from file.
     *
     * @param p_file
     *         the file containing the path
     * @param p_newFile
     *         the new file containing the path
     * @throws IOException
     *         if the log could not be moved
     */
    void renameLog(final File p_file, final File p_newFile) throws IOException;

    /**
     * Closes the log.
     *
     * @param p_log
     *         either a RandomAccessFile or an Integer storing the file id (DIR and RAW)
     * @throws IOException
     *         if the log could not be closed
     */
    void closeLog(final Object p_log) throws IOException;

    /**
     * Closes and deletes the log.
     *
     * @param p_log
     *         either a RandomAccessFile or an Integer storing the file id (DIR and RAW)
     * @param p_file
     *         the file containing the path
     * @throws IOException
     *         if the log could not be closed or removed
     */
    void closeAndRemoveLog(final Object p_log, final File p_file) throws IOException;

    /**
     * Reads from log.
     *
     * @param p_log
     *         either a RandomAccessFile or an Integer storing the file id (DIR and RAW)
     * @param p_bufferWrapper
     *         the buffer wrapper containing the byte buffer to be filled
     * @param p_length
     *         the number of bytes to read
     * @param p_readPos
     *         the position in log to read from
     * @throws IOException
     *         if the log could not be read
     */
    void read(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_length,
            final long p_readPos) throws IOException;

    /**
     * Writes to log (from arbitrary log position).
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
     * @param p_setLength
     *         whether the file should be truncated
     * @throws IOException
     *         if the log could not be written
     */
    void write(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length, final boolean p_setLength) throws IOException;

    /**
     * Writes to log (at the end).
     *
     * @param p_log
     *         either a RandomAccessFile or an Integer storing the file id (DIR and RAW)
     * @param p_bufferWrapper
     *         the buffer wrapper containing the byte buffer to be written
     * @param p_bufferOffset
     *         the offset within the byte buffer
     * @param p_writePos
     *         the end position of the log to append to
     * @param p_length
     *         the number of bytes to write
     * @throws IOException
     *         if the log could not be written
     */
    void append(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length) throws IOException;
}
