/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.utils;

/**
 * Implementation of JNI-Low-Level-Access to files with O_DIRECT Flag
 *
 * @author Christian Gesse <christian.gesse@hhu.de> 17.09.16
 */
public class JNIFileDirect {

    /**
     * Opens a new logfile or creates it if it doesnt exist
     *
     * @param p_path
     *     complete path to log including the filename
     * @param p_mode
     *     0 -> read/write, 1 -> read only
     * @return the filedescriptor or negative value on error
     */
    public static native int open(String p_path, int p_mode);

    /**
     * creates a new aligned buffer in native memory
     *
     * @param p_size
     *     length of buffer in bytes, must be a multiple of BLOCKSIZE
     * @return the address (pointer) of the created buffer or NULL
     */
    public static native long createBuffer(int p_size);

    /**
     * frees a created buffer
     *
     * @param p_ptr
     *     address of the buffer, interpreted as a pointer
     */
    public static native void freeBuffer(long p_ptr);

    /**
     * closes an open logfile
     *
     * @param p_fileID
     *     the file descriptor of the logfile
     * @return 0 on success, -1 on error
     */
    public static native int close(int p_fileID);

    /**
     * writes to the file from buffer data current position
     *
     * @param p_fileID
     *     the file descriptor
     * @param p_data
     *     reference to the byte buffer containing the data
     * @param p_offset
     *     start-offset in data-buffer
     * @param p_length
     *     number of bytes to write
     * @param p_position
     *     write-position in file
     * @param p_w_buf
     *     address of preallocated writebuffer
     * @param p_w_buf_size
     *     length of preallocated writebuffer
     * @return 0 on success or -1 on error
     */
    public static native int write(int p_fileID, byte[] p_data, int p_offset, int p_length, long p_position, long p_w_buf, int p_w_buf_size);

    /**
     * writes to the file from buffer data at current position and sets file length
     * -> use for VersionLog only because file is dynamically growing and only appended
     *
     * @param p_fileID
     *     the file descriptor
     * @param p_data
     *     reference to the byte buffer containing the data
     * @param p_offset
     *     start-offset in data-buffer
     * @param p_length
     *     number of bytes to write
     * @param p_position
     *     write-position in file
     * @param p_w_buf
     *     address of preallocated writebuffer
     * @param p_w_buf_size
     *     length of preallocated writebuffer
     * @return 0 on success or -1 on error
     */
    public static native int dwrite(int p_fileID, byte[] p_data, int p_offset, int p_length, long p_position, long p_w_buf, int p_w_buf_size);

    /**
     * reads from logfile into data-buffer at current position
     *
     * @param p_fileID
     *     the file descriptor
     * @param p_data
     *     reference to the byte buffer that should receive the data
     * @param p_offset
     *     start-offset in data-buffer
     * @param p_length
     *     number of bytes to read
     * @param p_position
     *     read-position in file
     * @param p_r_buf
     *     address of preallocated readbuffer
     * @param p_r_buf_size
     *     length of preallocated readbuffer
     * @return 0 on success or -1 on error
     */
    public static native int read(int p_fileID, byte[] p_data, int p_offset, int p_length, long p_position, long p_r_buf, int p_r_buf_size);

    /**
     * sets the filepointer to given value, measured from beginning of the file
     *
     * @param p_fileID:
     *     the file descriptor
     * @param p_position:
     *     new position in file
     * @return new position in file or -1 on errror
     */
    public static native long seek(int p_fileID, long p_position);

    /**
     * return length of the file
     *
     * @param p_fileID
     *     the descriptor of the fileID
     * @return the length of the file
     */
    public static native long length(int p_fileID);

    /**
     * sets the length of the file to a given length - padding with 0
     *
     * @param p_fileID
     *     file descriptor
     * @param p_fileLength
     *     new length of the file
     * @return 0 on success, -1 on error
     */
    public static native int setFileLength(int p_fileID, long p_fileLength);

    /**
     * a simple testfunction to check if jni-lib is loaded successfully
     */
    public static native void test();

}
