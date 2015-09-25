
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;

/**
 * Interface for log entry header
 * @author Kevin Beineke
 *         25.06.2015
 */
public interface LogEntryHeaderInterface {

	/**
	 * Generates a log entry with filled-in header but without any payload
	 * @param p_chunk
	 *            the Chunk
	 * @param p_rangeID
	 *            the RangeID
	 * @param p_source
	 *            the source NodeID
	 * @return the log entry
	 */
	byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source);

	/**
	 * Returns RangeID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the version
	 */
	byte getRangeID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns source of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the NodeID
	 */
	short getSource(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns NodeID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the NodeID
	 */
	short getNodeID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the LocalID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the LocalID
	 */
	long getLID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the ChunkID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the ChunkID
	 */
	long getChunkID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns length of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the length
	 */
	int getLength(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns version of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the version
	 */
	int getVersion(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the checksum of a log entry's payload
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the checksum
	 */
	long getChecksum(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the log entry header size
	 * @return the size
	 */
	short getHeaderSize();

	/**
	 * Returns the offset for conversion
	 * @return the offset
	 */
	short getConversionOffset();

	/**
	 * Returns the RangeID offset
	 * @return the offset
	 */
	short getRIDOffset();

	/**
	 * Returns the source offset
	 * @return the offset
	 */
	short getSRCOffset();

	/**
	 * Returns the NodeID offset
	 * @return the offset
	 */
	short getNIDOffset();

	/**
	 * Returns the LocalID offset
	 * @return the offset
	 */
	short getLIDOffset();

	/**
	 * Returns the length offset
	 * @return the offset
	 */
	short getLENOffset();

	/**
	 * Returns the version offset
	 * @return the offset
	 */
	short getVEROffset();

	/**
	 * Returns the checksum offset
	 * @return the offset
	 */
	short getCRCOffset();

	/**
	 * Prints the log header
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 */
	void print(final byte[] p_buffer, final int p_offset);
}
