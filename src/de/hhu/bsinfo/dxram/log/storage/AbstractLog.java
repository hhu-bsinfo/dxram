/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.utils.JNIFileDirect;
import de.hhu.bsinfo.utils.JNIFileRaw;

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

    private HarddriveAccessMode m_mode;
    private RandomAccessFile m_randomAccessFile;
    private int m_fileID;
    // Pointer to read- and writebuffer for JNI
    private long m_readBufferPointer;
    private long m_writeBufferPointer;
    // Size for read and write JNI-buffer
    private int m_readBufferSize;
    private int m_writeBufferSize;

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
     * @param p_mode
     *     the HarddriveAccessMode
     */
    AbstractLog(final File p_logFile, final long p_logSize, final int p_logHeaderSize, HarddriveAccessMode p_mode) {
        m_logFile = p_logFile;
        // header-size is not needed because header is not written to log
        //m_logFileSize = p_logSize + p_logHeaderSize;
        m_logFileSize = p_logSize;
        // set current header-length to 0, because header is not written to log
        //m_logFileHeaderSize = p_logHeaderSize;
        m_logFileHeaderSize = 0;
        m_totalUsableSpace = p_logSize;

        m_mode = p_mode;
        m_randomAccessFile = null;
        m_fileID = -1;

        if (m_mode != HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            // Set size for JNI-buffer (use 8 MB as standard)
            // Use only multiples of the device-blocksize to get an aligned buffer!
            m_writeBufferSize = 8 * 1024 * 1024;
            m_readBufferSize = 8 * 1024 * 1024;
        }

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
        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            return m_logFile.length();
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            return JNIFileDirect.length(m_fileID);
        } else {
            return JNIFileRaw.length(m_fileID);
        }
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

    /**
     * Key function to read from log file randomly - O_DIRECT or Raw version
     *
     * @param p_data
     *     buffer to fill with log data
     * @param p_length
     *     number of bytes to read
     * @param p_readPos
     *     the position within the log file
     * @param p_fileID
     *     the descriptor of the logfile
     * @param p_readBufferPointer
     *     a pointer to allocated readbuffer via JNI
     * @param p_readBufferSize
     *     the size of the allocated readbuffer
     * @param p_headerSize
     *     the length of the secondary log header
     * @param p_mode
     *     the HarddriveAccessMode
     */
    static void readFromSecondaryLogFile(final byte[] p_data, final int p_length, final long p_readPos, final int p_fileID, final long p_readBufferPointer,
        final int p_readBufferSize, final short p_headerSize, final HarddriveAccessMode p_mode) {
        if (p_mode == HarddriveAccessMode.ODIRECT) {
            final long innerLogSeekPos = p_headerSize + p_readPos;
            final long bytesUntilEnd = JNIFileDirect.length(p_fileID) - (p_headerSize + p_readPos);

            if (p_length > 0) {
                assert p_length <= bytesUntilEnd;

                JNIFileDirect.read(p_fileID, p_data, 0, p_length, innerLogSeekPos, p_readBufferPointer, p_readBufferSize);
            }
        } else {
            final long innerLogSeekPos = p_headerSize + p_readPos;
            final long bytesUntilEnd = JNIFileRaw.length(p_fileID) - (p_headerSize + p_readPos);

            if (p_length > 0) {
                assert p_length <= bytesUntilEnd;

                JNIFileRaw.read(p_fileID, p_data, 0, p_length, innerLogSeekPos, p_readBufferPointer, p_readBufferSize);

            }
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
        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            m_randomAccessFile.close();
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            if (JNIFileDirect.close(m_fileID) < 0) {
                throw new IOException("Error Closing the log");
            }
            JNIFileDirect.freeBuffer(m_readBufferPointer);
            JNIFileDirect.freeBuffer(m_writeBufferPointer);
        } else {
            if (JNIFileRaw.closeLog(m_fileID) < 0) {
                throw new IOException("Error Closing the log");
            }
            JNIFileRaw.freeBuffer(m_readBufferPointer);
            JNIFileRaw.freeBuffer(m_writeBufferPointer);
        }
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

        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
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
        } else if (m_mode == HarddriveAccessMode.ODIRECT) {
            if (m_logFile.exists()) {
                ret = m_logFile.delete();
            }

            if (ret && !m_logFile.getParentFile().exists()) {
                // Create folders
                ret = m_logFile.getParentFile().mkdirs();
            }

            if (ret) {
                // Write header
                String path = m_logFile.getCanonicalPath();
                m_fileID = JNIFileDirect.open(path, 0);
                // Check for error in native Code
                if (m_fileID < 0) {
                    throw new IOException("JNI error: Cannot create or open log file");
                }
                JNIFileDirect.seek(m_fileID, 0);
                // Set length of file
                //JNIFileDirect.seek(m_fileID, 0);
                JNIFileDirect.setFileLength(m_fileID, m_logFileSize);
                // Create buffer
                m_readBufferPointer = JNIFileDirect.createBuffer(m_readBufferSize);
                m_writeBufferPointer = JNIFileDirect.createBuffer(m_writeBufferSize);

                JNIFileDirect.length(m_fileID);
            }
        } else {
            // Write header
            String fileName = m_logFile.getName();
            m_fileID = JNIFileRaw.createLog(fileName, m_logFileSize);
            // Check for error in native Code
            if (m_fileID < 0) {
                throw new IOException("JNI error: Cannot create or open log file");
            }
            // Create buffer
            m_readBufferPointer = JNIFileRaw.createBuffer(m_readBufferSize);
            m_writeBufferPointer = JNIFileRaw.createBuffer(m_writeBufferSize);
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

            if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                m_randomAccessFile.seek(innerLogSeekPos);
                m_randomAccessFile.readFully(p_data, 0, p_length);
            } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                if (JNIFileDirect.read(m_fileID, p_data, 0, p_length, innerLogSeekPos, m_readBufferPointer, m_readBufferSize) < 0) {
                    throw new IOException("Error reading from log");
                }
            } else {
                if (JNIFileRaw.read(m_fileID, p_data, 0, p_length, innerLogSeekPos, m_readBufferPointer, m_readBufferSize) < 0) {
                    throw new IOException("Error reading from log");
                }
            }

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
            if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                m_randomAccessFile.seek(m_logFileHeaderSize + p_writePos);
                m_randomAccessFile.write(p_data, p_bufferOffset, p_length);
            } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                if (JNIFileDirect.write(m_fileID, p_data, p_bufferOffset, p_length, m_logFileHeaderSize + p_writePos, m_writeBufferPointer, m_writeBufferSize) <
                    0) {
                    throw new IOException("Error writing to log");
                }
            } else {
                if (JNIFileRaw.write(m_fileID, p_data, p_bufferOffset, p_length, m_logFileHeaderSize + p_writePos, m_writeBufferPointer, m_writeBufferSize) <
                    0) {
                    throw new IOException("Error writing to log");
                }
            }

            ret = p_writePos + p_length;
        } else {
            // Twofold cyclic write access
            // NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
            bytesUntilEnd = m_totalUsableSpace - p_writePos;

            if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                m_randomAccessFile.seek(m_logFileHeaderSize + p_writePos);
                m_randomAccessFile.write(p_data, p_bufferOffset, (int) bytesUntilEnd);
                m_randomAccessFile.seek(m_logFileHeaderSize);
                m_randomAccessFile.write(p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
            } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                if (JNIFileDirect
                    .write(m_fileID, p_data, p_bufferOffset, (int) bytesUntilEnd, m_logFileHeaderSize + p_writePos, m_writeBufferPointer, m_writeBufferSize) <
                    0) {
                    throw new IOException("Error writing to log");
                }
                if (JNIFileDirect
                    .write(m_fileID, p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd, m_logFileHeaderSize, m_writeBufferPointer,
                        m_writeBufferSize) < 0) {
                    throw new IOException("Error writing to log");
                }
            } else {
                if (JNIFileRaw
                    .write(m_fileID, p_data, p_bufferOffset, (int) bytesUntilEnd, m_logFileHeaderSize + p_writePos, m_writeBufferPointer, m_writeBufferSize) <
                    0) {
                    throw new IOException("Error writing to log");
                }
                if (JNIFileRaw
                    .write(m_fileID, p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd, m_logFileHeaderSize, m_writeBufferPointer,
                        m_writeBufferSize) < 0) {
                    throw new IOException("Error writing to log");
                }
            }

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

            if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
                m_randomAccessFile.seek(m_logFileHeaderSize + p_readPos);
                m_randomAccessFile.write(p_data, p_bufferOffset, p_length);
            } else if (m_mode == HarddriveAccessMode.ODIRECT) {
                if (JNIFileDirect.write(m_fileID, p_data, p_bufferOffset, p_length, m_logFileHeaderSize + p_readPos, m_writeBufferPointer, m_writeBufferSize) <
                    0) {
                    throw new IOException("Error writing to log");
                }
            } else {
                if (JNIFileRaw.write(m_fileID, p_data, p_bufferOffset, p_length, m_logFileHeaderSize + p_readPos, m_writeBufferPointer, m_writeBufferSize) <
                    0) {
                    throw new IOException("Error writing to log");
                }
            }

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
