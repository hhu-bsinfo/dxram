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

package de.hhu.bsinfo.utils.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * File class implementing IODataStream.
 * This class is mainly a wrapper for the RandomAccessFile class
 * provided by the java api plus some additional features and convenience.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 08.03.2016
 */
public class IODataStreamFile extends AbstractIODataStream {
    public static final String MS_TYPE = "File";
    private RandomAccessFile m_fileHandle;

    /**
     * Constructor
     *
     * @param p_filename
     *         Name of the file (full path).
     */
    public IODataStreamFile(final String p_filename) {
        super(MS_TYPE, p_filename);
    }

    @Override public ErrorCode open(final OperationMode p_mode, final boolean p_overwrite) {
        // check if already opened
        if (getMode() != OperationMode.INVALID) {
            return ErrorCode.ALREADY_OPENED;
        }

        // create string for random access file according to selected mode
        String modeString = "";
        switch (p_mode) {
            case READ:
                modeString = "r";
                break;
            case WRITE:
                modeString = "rw";
                break;
            default:
                break;
        }

        // only apply this in write mode
        if (p_overwrite && p_mode == OperationMode.WRITE) {
            // check if file exists and delete it
            File file = new File(getAddress());
            if (file.exists()) {
                if (!file.delete()) {
                    return ErrorCode.ACCESS_DENIED;
                }
            }
        }

        // create random access file
        try {
            m_fileHandle = new RandomAccessFile(getAddress(), modeString);
        } catch (final FileNotFoundException e) {
            return ErrorCode.ACCESS_DENIED;
        }

        // just in case, reset pointer
        try {
            m_fileHandle.seek(0);
        } catch (final IOException e) {
            return ErrorCode.UNKNOWN;
        }

        setMode(p_mode);
        return ErrorCode.SUCCESS;
    }

    @Override public ErrorCode close() {
        assert getMode() != OperationMode.INVALID;

        try {
            m_fileHandle.close();
        } catch (final IOException e) {
            return ErrorCode.UNKNOWN;
        }

        // don't forget to destroy the object
        m_fileHandle = null;
        setMode(OperationMode.INVALID);
        return ErrorCode.SUCCESS;
    }

    @Override public boolean isOpened() {
        return getMode() != OperationMode.INVALID;
    }

    @Override public int read(final byte[] p_data, final int p_offset, final int p_size) {
        assert getMode() != OperationMode.INVALID;

        int bytesRead = -1;

        try {
            bytesRead = m_fileHandle.read(p_data, p_offset, p_size);
        } catch (final IOException e) {
            return -1;
        }

        if (bytesRead == -1) {
            return 0;
        } else {
            return bytesRead;
        }
    }

    @Override public int write(final byte[] p_data, final int p_offset, final int p_size) {
        assert getMode() != OperationMode.INVALID;

        // check if the right mode is set
        if (getMode() == OperationMode.READ) {
            return -2;
        }

        try {
            m_fileHandle.write(p_data, p_offset, p_size);
        } catch (final IOException e1) {
            return -1;
        }

        return p_size;
    }

    @Override public boolean seek(final long p_pos) {
        assert getMode() != OperationMode.INVALID;

        try {
            m_fileHandle.seek(p_pos);
        } catch (final IOException e) {
            return false;
        }

        return true;
    }

    @Override public boolean eof() {
        assert getMode() != OperationMode.INVALID;

        try {
            return m_fileHandle.getFilePointer() >= m_fileHandle.length();
        } catch (final IOException e) {
            return true;
        }
    }

    @Override public long tell() {
        assert getMode() != OperationMode.INVALID;

        try {
            return m_fileHandle.getFilePointer();
        } catch (final IOException e) {
            return -1;
        }
    }

    @Override public long size() {
        assert getMode() != OperationMode.INVALID;

        try {
            return m_fileHandle.length();
        } catch (final IOException e) {
            return -1;
        }
    }

    @Override public boolean flush() {
        // not used
        return true;
    }
}
