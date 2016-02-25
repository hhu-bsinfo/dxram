package de.hhu.bsinfo.utils.serialization;

/** Interface for an instance which can export/serialize
 *  Objects. This instance can (for example) write the contents
 *  of an object to a file.
 *  @author Stefan Nothaas 17.12.15 <stefan.nothaas@hhu.de>
 */
public interface Exporter 
{		
	/** Export the provided exportable object to this target.
	 * 
	 *  Depending on the implementation this calls at least
	 *  the exportObject method of the exportable object. 
	 *  But it's possible to trigger some pre- and post processing
	 *  of data/buffers.
	 * 
	 * @param p_object Exportable object to serialize/write to this target.
	 * @return Number of bytes written to the exporter i.e. the size of the written object.
	 */
	int exportObject(final Exportable p_object);
	
	// ----------------------------------------------------------------------
	
	/** Write a single byte to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_v Byte to write.
	 */
	void writeByte(final byte p_v);
	
	/** Write a short to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_v Value to write.
	 */
	void writeShort(final short p_v);
	
	/** Write an int to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_v Value to write.
	 */
	void writeInt(final int p_v);
	
	/** Write a long to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_v Value to write.
	 */
	void writeLong(final long p_v);
	
	/** Write a float to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_v Value to write.
	 */
	void writeFloat(final float p_v);
	
	/** Write a double to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_v Value to write.
	 */
	void writeDouble(final double p_v);
	
	/** Write a byte array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @return Number of written elements.
	 */
	int writeBytes(final byte[] p_array);
	
	/** Write a short array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @return Number of written elements.
	 */
	int writeShorts(final short[] p_array);
	
	/** Write an int array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @return Number of written elements.
	 */
	int writeInts(final int[] p_array);
	
	/** Write a long array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @return Number of written elements.
	 */
	int writeLongs(final long[] p_array);
	
	/** Write a byte array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @param p_offset Offset to start writing from.
	 * @param p_length Number of elements to write.
	 * @return Number of written elements.
	 */
	int writeBytes(final byte[] p_array, final int p_offset, final int p_length);
	
	/** Write a short array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @param p_offset Offset to start writing from.
	 * @param p_length Number of elements to write.
	 * @return Number of written elements.
	 */
	int writeShorts(final short[] p_array, final int p_offset, final int p_length);
	
	/** Write an int array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @param p_offset Offset to start writing from.
	 * @param p_length Number of elements to write.
	 * @return Number of written elements.
	 */
	int writeInts(final int[] p_array, final int p_offset, final int p_length);
	
	/** Write a long array to the target.
	 * 
	 *  Use this call in your exportable object in the
	 *  export call to write data to the target.
	 *  
	 * @param p_array Array to write.
	 * @param p_offset Offset to start writing from.
	 * @param p_length Number of elements to write.
	 * @return Number of written elements.
	 */
	int writeLongs(final long[] p_array, final int p_offset, final int p_length);
}
