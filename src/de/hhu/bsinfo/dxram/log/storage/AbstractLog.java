package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Skeleton for a log
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2014
 */
public abstract class AbstractLog {

    // Attributes
    private final long m_logFileSize;
    private long m_totalUsableSpace;
    private int m_logFileHeaderSize;
    private File m_logFile;
    private RandomAccessFile m_randomAccessFile;
    private ReentrantLock m_lock;

    // Constructors

    /**
     * Initializes the common resources of a log
     *
     * @param p_logFile
     *     the random access file (RAF) of the log
     * @param p_logSize
     *     the size in byte of the log
     * @param p_logHeaderSize
     *     the size in byte of the log header
     */
    AbstractLog(final File p_logFile, final long p_logSize, final int p_logHeaderSize) {
        m_logFile = p_logFile;
        m_logFileSize = p_logSize + p_logHeaderSize;
        m_logFileHeaderSize = p_logHeaderSize;
        m_totalUsableSpace = p_logSize;

        m_randomAccessFile = null;

        m_lock = new ReentrantLock(false);
    }

    /**
     * Returns the number of bytes in log
     *
     * @return the number of bytes in log
     */
    abstract long getOccupiedSpace();

    /**
     * Returns the log file's size
     *
     * @return the size
     */
    final long getFileSize() {
        return m_logFile.length();
    }

    // Getter

    /**
     * Key function to read from log file randomly
     *
     * @param p_data
     *     buffer to fill with log data
     * @param p_length
     *     number of bytes to read
     * @param p_readPos
     *     the position within the log file
     * @param p_randomAccessFile
     *     the RandomAccessFile
     * @param p_headerSize
     *     the length of the secondary log header
     * @throws IOException
     *     if reading the random access file failed
     */
    static void readFromSecondaryLogFile(final byte[] p_data, final int p_length, final long p_readPos, final RandomAccessFile p_randomAccessFile,
        final short p_headerSize) throws IOException {
        final long innerLogSeekPos = p_headerSize + p_readPos;
        final long bytesUntilEnd = p_randomAccessFile.length() - (p_headerSize + p_readPos);

        if (p_length > 0) {
            assert p_length <= bytesUntilEnd;

            p_randomAccessFile.seek(innerLogSeekPos);
            p_randomAccessFile.readFully(p_data, 0, p_length);
        }
    }

    // Methods

    /**
     * Opens a random access file for the log
     *
     * @param p_logFile
     *     the log file
     * @return file descriptor to the log file
     * @throws IOException
     *     if opening the random access file failed
     */
    private static RandomAccessFile openLog(final File p_logFile) throws IOException {
        return new RandomAccessFile(p_logFile, "rw");
    }

    /**
     * Closes the log
     *
     * @throws IOException
     *     if the flushing during closure fails
     */
    public final void closeLog() throws IOException {
        m_randomAccessFile.close();
    }

    /**
     * Creates and initializes random access file
     *
     * @param p_header
     *     the log type specific header
     * @return whether the log could be created
     * @throws IOException
     *     if the header could not be read or written
     */
    final boolean createLogAndWriteHeader(final byte[] p_header) throws IOException {
        boolean ret = true;

        if (m_logFile.exists()) {
            ret = m_logFile.delete();
        }

        if (ret && !m_logFile.getParentFile().exists()) {
            // Create folders
            ret = m_logFile.getParentFile().mkdirs();
        }

        if (ret) {
            // Create file
            ret = m_logFile.createNewFile();

            // Write header
            m_randomAccessFile = openLog(m_logFile);
            m_randomAccessFile.seek(0);
            m_randomAccessFile.write(p_header);

            m_randomAccessFile.seek(0);
            m_randomAccessFile.setLength(m_logFileSize);
        }

        return ret;
    }

    /**
     * Key function to read from log randomly
     *
     * @param p_data
     *     buffer to fill with log data
     * @param p_length
     *     number of bytes to read
     * @param p_readPos
     *     the position within the log file
     * @param p_accessed
     *     whether the RAF is accessed by another thread or not
     * @throws IOException
     *     if reading the random access file failed
     */
    final void readFromSecondaryLog(final byte[] p_data, final int p_length, final long p_readPos, final boolean p_accessed) throws IOException {
        final long innerLogSeekPos = m_logFileHeaderSize + p_readPos;
        final long bytesUntilEnd = m_totalUsableSpace - p_readPos;

        if (p_length > 0) {
            if (p_accessed) {
                m_lock.lock();
            }

            assert p_length <= bytesUntilEnd;

            m_randomAccessFile.seek(innerLogSeekPos);
            m_randomAccessFile.readFully(p_data, 0, p_length);

            if (p_accessed) {
                m_lock.unlock();
            }
        }
    }

    /**
     * Key function to write in log sequentially
     *
     * @param p_data
     *     buffer with data to write in log
     * @param p_bufferOffset
     *     offset in buffer
     * @param p_length
     *     number of bytes to write
     * @param p_writePos
     *     the current write position
     * @return updated write position
     * @throws IOException
     *     if reading the random access file failed
     */
    final long appendToPrimaryLog(final byte[] p_data, final int p_bufferOffset, final int p_length, final long p_writePos) throws IOException {
        long ret;
        final long bytesUntilEnd;

        if (p_writePos + p_length <= m_totalUsableSpace) {
            m_randomAccessFile.seek(m_logFileHeaderSize + p_writePos);
            m_randomAccessFile.write(p_data, p_bufferOffset, p_length);
            ret = p_writePos + p_length;
        } else {
            // Twofold cyclic write access
            // NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
            bytesUntilEnd = m_totalUsableSpace - p_writePos;
            m_randomAccessFile.seek(m_logFileHeaderSize + p_writePos);
            m_randomAccessFile.write(p_data, p_bufferOffset, (int) bytesUntilEnd);
            m_randomAccessFile.seek(m_logFileHeaderSize);
            m_randomAccessFile.write(p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
            ret = p_length - bytesUntilEnd;
        }

        return ret;
    }

    /**
     * Key function to write in log
     *
     * @param p_data
     *     buffer with data to write in log
     * @param p_bufferOffset
     *     offset in buffer
     * @param p_readPos
     *     offset in log file
     * @param p_length
     *     number of bytes to write
     * @param p_accessed
     *     whether the RAF is accessed by another thread or not
     * @return number of bytes that were written successfully
     * @throws IOException
     *     if reading the random access file failed
     */
    final int writeToSecondaryLog(final byte[] p_data, final int p_bufferOffset, final long p_readPos, final int p_length, final boolean p_accessed)
        throws IOException {

        if (p_length > 0) {
            if (p_accessed) {
                m_lock.lock();
            }

            assert p_readPos + p_length <= m_totalUsableSpace;

            m_randomAccessFile.seek(m_logFileHeaderSize + p_readPos);
            m_randomAccessFile.write(p_data, p_bufferOffset, p_length);

            if (p_accessed) {
                m_lock.unlock();
            }
        }
        return p_length;
    }

    /**
     * Writes data to log
     *
     * @param p_data
     *     a buffer
     * @param p_offset
     *     offset within the buffer
     * @param p_length
     *     length of data
     * @return number of successfully written bytes
     * @throws InterruptedException
     *     if the write access fails
     * @throws IOException
     *     if the write access fails
     */
    abstract int appendData(byte[] p_data, int p_offset, int p_length) throws IOException, InterruptedException;
}
