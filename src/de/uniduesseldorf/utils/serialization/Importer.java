package de.uniduesseldorf.utils.serialization;

/** Interface for an instance which can import/de-serialize
 *  Objects. This instance can (for example) read the contents
 *  of an object from a file.
 *  @author Stefan Nothaas 17.12.15 <stefan.nothaas@hhu.de>
 */
public interface Importer 
{
	/** Import/read data from the target to the provided already
	 *  allocated object.
	 * 
	 *  Depending on the implementation this calls at least
	 *  the importObject method of the importable object. 
	 *  But it's possible to trigger some pre- and post processing
	 *  of data/buffers.
	 * 
	 * @param p_object Importable Pre-allocated object to read data from the target into.
	 * @return Number of bytes read from the importer i.e. the size of the read object.
	 */
	int importObject(final Importable p_object);
	
	// ----------------------------------------------------------------------
	
	/** Read a single byte from the target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @return Byte read.
	 */
	byte readByte();
	
	/** Read a short from the target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @return Value read.
	 */
	short readShort();
	
	/** Read an int from the target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @return Value read.
	 */
	int readInt();
	
	/** Read a long from the target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @return Value read.
	 */
	long readLong();
	
	/** Read a float from the target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @return Value read.
	 */
	float readFloat();
	
	/** Read a double from the target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @return Value read.
	 */
	double readDouble();
	
	/** Read data into a byte array from target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @param p_array Array to read into.
	 * @return Number of read bytes into the array.
	 */
	int readBytes(final byte[] p_array);
	
	/** Read data into a byte array from target.
	 * 
	 *  Use this call in your importable object in the
	 *  import call to read data from the target.
	 *  
	 * @param p_array Array to read into.
	 * @param p_offset Offset to start in the array for reading into.
	 * @param p_length Number of bytes to read.
	 * @return Number of read bytes into the array.
	 */
	int readBytes(final byte[] p_array, final int p_offset, int p_length);
}