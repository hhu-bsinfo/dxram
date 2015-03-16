package de.uniduesseldorf.dxram.core.log.storage;

import java.io.IOException;

/**
 * Methods for storing the data in log
 * @author Kevin Beineke
 *         26.05.2014
 */
public interface LogStorageInterface {

	// Methods
	/**
	 * Closes the log
	 * @throws InterruptedException
	 *            if the closure fails
	 * @throws IOException
	 *            if the flushing during closure fails
	 */
	void closeLog() throws InterruptedException, IOException;

	/**
	 * Writes data in log sequentially
	 * @param p_data
	 *            a buffer
	 * @param p_offset
	 *            offset within the buffer
	 * @param p_length
	 *            length of data
	 * @param p_additionalInformation
	 *            place holder for additional information
	 * @throws InterruptedException
	 *            if the write access fails
	 * @throws IOException
	 *            if the write access fails
	 * @return number of successfully written bytes
	 */
	int appendData(final byte[] p_data, final int p_offset, final int p_length,
			final Object p_additionalInformation) throws IOException, InterruptedException;

}
