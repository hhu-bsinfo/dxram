package de.uniduesseldorf.dxram.core.mem;

/**
 * Interface for any kind of data structure that can be stored and read from 
 * memory. Implement this with any object you want to put/get from the memory system.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public interface DataStructure 
{
	/**
	 * Get the unique identifier of this data structure.
	 * @return Unique identifier.
	 */
	public long getID();
	
	/**
	 * Write the payload/data of the data structure to the given writer. No total size
	 * of the data structure needs to be written to the buffer.
	 * @param p_startAddress Start address for writing the payload. Pass this on to the writer.
	 * @param p_writer The writer to write the data to.
	 */
	public int writePayload(final long p_startAddress, final DataStructureWriter p_writer);
	
	/**
	 * Read the payload/data for this data structure from the given reader.
	 * @param p_startAddress Start address for reading the data. Pass this to the reader.
	 * @param p_reader The reader to read the data from.
	 */
	public int readPayload(final long p_startAddress, final int p_dataLength, final DataStructureReader p_reader);
	
	/**
	 * Get the total number of bytes the payload needs.
	 * This does not include the ID or length of the complete data structure.
	 * @return Size of the payload.
	 */
	public int sizeofPayload();
}
