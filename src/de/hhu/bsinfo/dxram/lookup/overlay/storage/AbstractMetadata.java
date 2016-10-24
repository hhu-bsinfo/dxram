
package de.hhu.bsinfo.dxram.lookup.overlay.storage;

/**
 * Skeleton for superpeer metadata
 * @author Kevin Beineke 12.10.2016
 */
public abstract class AbstractMetadata {

	/**
	 * Stores all metadata from given byte array
	 * @param p_data
	 *            the byte array
	 * @param p_offset
	 *            the offset within the byte array
	 * @param p_size
	 *            the number of bytes
	 * @return the amount of stored metadata
	 */
	public abstract int storeMetadata(final byte[] p_data, final int p_offset, final int p_size);

	/**
	 * Returns all metadata
	 * @return all metadata in a byte array
	 */
	public abstract byte[] receiveAllMetadata();

	/**
	 * Returns all entries in area
	 * @param p_bound1
	 *            the first bound
	 * @param p_bound2
	 *            the second bound
	 * @return corresponding metadata in a byte array
	 */
	public abstract byte[] receiveMetadataInRange(final short p_bound1, final short p_bound2);

	/**
	 * Removes all metadata outside of area
	 * @param p_bound1
	 *            the first bound
	 * @param p_bound2
	 *            the second bound
	 * @return number of removed metadata
	 */
	public abstract int removeMetadataOutsideOfRange(final short p_bound1, final short p_bound2);

	/**
	 * Returns the amount of metadata in area
	 * @param p_bound1
	 *            the first bound
	 * @param p_bound2
	 *            the second bound
	 * @return the amount of metadata
	 */
	public abstract int quantifyMetadata(final short p_bound1, final short p_bound2);
}