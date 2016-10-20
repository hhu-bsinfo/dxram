
package de.hhu.bsinfo.utils.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * File class implementing IODataStream.
 * This class is mainly a wrapper for the RandomAccessFile class
 * provided by the java api plus some additional features and convenience.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 08.03.16
 */
public class IODataStreamFile extends IODataStream {
	public static final String MS_TYPE = "File";
	private RandomAccessFile m_fileHandle;

	/**
	 * Constructor
	 * @param p_filename
	 *            Name of the file (full path).
	 */
	public IODataStreamFile(final String p_filename) {
		super(MS_TYPE, p_filename);
	}

	@Override
	public ERROR_CODE open(final OPERATION_MODE p_mode, final boolean p_overwrite) {
		// check if already opened
		if (m_mode != OPERATION_MODE.INVALID) {
			return ERROR_CODE.ALREADY_OPENED;
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
		if (p_overwrite && p_mode == OPERATION_MODE.WRITE) {
			// check if file exists and delete it
			File file = new File(getAddress());
			if (file.exists()) {
				if (file.delete() == false) {
					return ERROR_CODE.ACCESS_DENIED;
				}
			}
		}

		// create random access file
		try {
			m_fileHandle = new RandomAccessFile(getAddress(), modeString);
		} catch (FileNotFoundException e) {
			return ERROR_CODE.ACCESS_DENIED;
		}

		// just in case, reset pointer
		try {
			m_fileHandle.seek(0);
		} catch (java.io.IOException e) {
			return ERROR_CODE.UNKNOWN;
		}

		m_mode = p_mode;
		return ERROR_CODE.SUCCESS;
	}

	@Override
	public ERROR_CODE close() {
		assert m_mode != OPERATION_MODE.INVALID;

		try {
			m_fileHandle.close();
		} catch (java.io.IOException e) {
			return ERROR_CODE.UNKNOWN;
		}

		// don't forget to destroy the object
		m_fileHandle = null;
		m_mode = OPERATION_MODE.INVALID;
		return ERROR_CODE.SUCCESS;
	}

	@Override
	public boolean isOpened() {
		if (m_mode == OPERATION_MODE.INVALID) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public int read(final byte[] p_data, final int p_offset, final int p_size) {
		assert m_mode != OPERATION_MODE.INVALID;

		int bytesRead = -1;

		try {
			bytesRead = m_fileHandle.read(p_data, p_offset, p_size);
		} catch (java.io.IOException e) {
			return -1;
		}

		if (bytesRead == -1) {
			return 0;
		} else {
			return bytesRead;
		}
	}

	@Override
	public int write(final byte[] p_data, final int p_offset, final int p_size) {
		assert m_mode != OPERATION_MODE.INVALID;

		// check if the right mode is set
		if (m_mode == OPERATION_MODE.READ) {
			return -2;
		}

		try {
			m_fileHandle.write(p_data, p_offset, p_size);
		} catch (java.io.IOException e1) {
			return -1;
		}

		return p_size;
	}

	@Override
	public boolean seek(final long p_pos) {
		assert m_mode != OPERATION_MODE.INVALID;

		try {
			m_fileHandle.seek(p_pos);
		} catch (java.io.IOException e) {
			return false;
		}

		return true;
	}

	@Override
	public boolean eof() {
		assert m_mode != OPERATION_MODE.INVALID;

		try {
			if (m_fileHandle.getFilePointer() < m_fileHandle.length()) {
				return false;
			} else {
				return true;
			}
		} catch (java.io.IOException e) {
			return true;
		}
	}

	@Override
	public long tell() {
		assert m_mode != OPERATION_MODE.INVALID;

		try {
			return m_fileHandle.getFilePointer();
		} catch (java.io.IOException e) {
			return -1;
		}
	}

	@Override
	public long size() {
		assert m_mode != OPERATION_MODE.INVALID;

		try {
			return m_fileHandle.length();
		} catch (java.io.IOException e) {
			return -1;
		}
	}

	@Override
	public boolean flush() {
		// not used
		return true;
	}
}
