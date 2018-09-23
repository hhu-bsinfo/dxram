/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.logs;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.diskaccess.DirectDiskAccess;
import de.hhu.bsinfo.dxram.log.storage.diskaccess.DiskAccessInterface;
import de.hhu.bsinfo.dxram.log.storage.diskaccess.FileDiskAccess;
import de.hhu.bsinfo.dxram.log.storage.diskaccess.RawDiskAccess;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionBuffer;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;

/**
 * Skeleton for a log.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2014
 */
public class Log {

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionBuffer.class.getSimpleName());

    protected static DiskAccessInterface ms_logAccess;

    protected Object m_log;
    protected long m_logSize;

    private File m_file;

    /**
     * Initializes the common resources of a log
     *
     * @param p_logFile
     *         the file
     * @param p_logSize
     *         the size of the log in bytes
     */
    public Log(final File p_logFile, final long p_logSize) {
        m_file = p_logFile;
        m_logSize = p_logSize;
    }

    /**
     * Sets the access mode. Must be called before the first log is created!
     *
     * @param p_mode
     *         the hard drive access mode
     */
    public static void setAccessMode(final HarddriveAccessMode p_mode) {
        if (p_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            ms_logAccess = new FileDiskAccess();
        } else if (p_mode == HarddriveAccessMode.ODIRECT) {
            ms_logAccess = new DirectDiskAccess();
        } else {
            ms_logAccess = new RawDiskAccess();
        }
    }

    /**
     * Returns the number of bytes in log
     *
     * @return the number of bytes in log
     */
    public long getOccupiedSpace() {
        return getFileSize();
    }

    /**
     * Returns the log file's size
     *
     * @return the size
     */
    public final long getFileSize() {
        long ret = -1;

        try {
            ret = ms_logAccess.getFileSize(m_log);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    /**
     * Closes the log
     *
     * @throws IOException
     *         if the closing fails
     */
    public final void close() throws IOException {
        ms_logAccess.closeLog(m_log);
    }

    /**
     * Closes the log and deletes it
     *
     * @throws IOException
     *         if the closing fails
     */
    public void closeAndRemove() throws IOException {
        ms_logAccess.closeAndRemoveLog(m_log, m_file);
    }

    /**
     * Creates and initializes the log
     *
     * @throws IOException
     *         if the header could not be read or written
     */
    protected final void createLog() throws IOException {
        m_log = ms_logAccess.createLog(m_file, m_logSize);

        if (m_log == null) {
            throw new IOException("Log could not be created.");
        }
    }

    /**
     * Renames the log. Used after the recovery.
     *
     * @throws IOException
     *         if the file could not be renamed.
     */
    protected final void renameLog(final File p_newFile) throws IOException {
        ms_logAccess.renameLog(m_file, p_newFile);
        m_file = p_newFile;
    }

    /**
     * Key function to read from log randomly
     *
     * @param p_bufferWrapper
     *         page-aligned buffer to fill with log data
     * @param p_length
     *         number of bytes to read
     * @param p_readPos
     *         the position within the log file
     * @throws IOException
     *         if reading from log failed
     */
    public void readFromLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_length, final long p_readPos)
            throws IOException {

        LOGGER.error("The log type %s does not support reading from", getClass().getSimpleName());
        throw new IOException();
    }

    /**
     * Key function to write to log.
     *
     * @param p_bufferWrapper
     *         buffer with data to write to log
     * @param p_bufferOffset
     *         offset in buffer
     * @param p_writePos
     *         offset in log file
     * @param p_length
     *         number of bytes to write
     * @param p_accessed
     *         whether the log might be accessed concurrently by another thread or not
     * @throws IOException
     *         if reading the from log failed
     */
    public void writeToLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length, final boolean p_accessed) throws IOException {

        LOGGER.error("The log type %s does not support writing to", getClass().getSimpleName());
        throw new IOException();
    }

    /**
     * Key function to write to log sequentially.
     *
     * @param p_bufferWrapper
     *         buffer with data to write to log
     * @param p_bufferOffset
     *         offset in buffer
     * @param p_length
     *         number of bytes to write
     * @return updated write position
     * @throws IOException
     *         if writing to log failed
     */
    public long appendToLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset, int p_length)
            throws IOException {

        LOGGER.error("The log type %s does not support appending to", getClass().getSimpleName());
        throw new IOException();
    }

    /**
     * Opens a log from file.
     *
     * @throws IOException
     *         if the header could not be read or written
     */
    public static Object openLog(final File p_file) throws IOException {
        Object log = ms_logAccess.openLog(p_file);

        if (log == null) {
            throw new IOException("Log could not be created.");
        }

        return log;
    }

    /**
     * Closes a log initialized from file.
     *
     * @param p_log
     *         the log (either a RandomAccessFile or Integer storing the file id)
     * @throws IOException
     *         if the file could not be closed
     */
    public static void closeLog(final Object p_log) throws IOException {
        ms_logAccess.closeLog(p_log);
    }

    /**
     * Key function to read from log file.
     *
     * @param p_log
     *         the log (either a RandomAccessFile or Integer storing the file id)
     * @param p_bufferWrapper
     *         page-aligned buffer to fill with log data
     * @param p_length
     *         number of bytes to read
     * @param p_readPos
     *         the position within the log file
     */
    public static void readFromFile(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper,
            final int p_length, final long p_readPos) throws IOException {

        final long bytesUntilEnd = ms_logAccess.getFileSize(p_log) - p_readPos;

        if (p_length > 0) {
            assert p_length <= bytesUntilEnd;

            ms_logAccess.read(p_log, p_bufferWrapper, p_length, p_readPos);
        }
    }
}
