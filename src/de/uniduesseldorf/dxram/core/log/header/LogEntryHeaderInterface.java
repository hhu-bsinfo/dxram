
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
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the NodeID
	 */
	short getNodeID(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);

	/**
	 * Returns the LID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the LID
	 */
	long getLID(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);

	/**
	 * Returns the ChunkID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the ChunkID
	 */
	long getChunkID(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);

	/**
	 * Returns length of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the length
	 */
	int getLength(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);

	/**
	 * Returns version of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the version
	 */
	int getVersion(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);

	/**
	 * Returns the checksum of a log entry's payload
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the checksum
	 */
	long getChecksum(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);

	/**
	 * Returns the log entry header size
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the size
	 */
	short getHeaderSize(final boolean... p_logStoresMigrations);

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
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the offset
	 */
	short getNIDOffset(final boolean... p_logStoresMigrations);

	/**
	 * Returns the LocalID offset
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the offset
	 */
	short getLIDOffset(final boolean... p_logStoresMigrations);

	/**
	 * Returns the length offset
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the offset
	 */
	short getLENOffset(final boolean... p_logStoresMigrations);

	/**
	 * Returns the version offset
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the offset
	 */
	short getVEROffset(final boolean... p_logStoresMigrations);

	/**
	 * Returns the checksum offset
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 * @return the offset
	 */
	short getCRCOffset(final boolean... p_logStoresMigrations);

	/**
	 * Prints the log header
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 *            Important: this parameter must be set for secondary log entry headers only
	 */
	void print(final byte[] p_buffer, final int p_offset, final boolean... p_logStoresMigrations);
}
