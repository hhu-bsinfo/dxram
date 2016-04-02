
package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Stores locations
 * @author Kevin Beineke
 *         03.09.2013
 */
public final class LookupRange implements Importable, Exportable {

	// Attributes
	private short m_primaryPeer;
	private long[] m_range;

	/**
	 * Default constructor
	 */
	public LookupRange() {
		m_primaryPeer = -1;
		m_range = null;
	}

	// Constructors
	/**
	 * Creates an instance of Locations
	 * @param p_primaryPeer
	 *            the primary peer
	 * @param p_range
	 *            the range's beginning and ending
	 */
	public LookupRange(final short p_primaryPeer, final long[] p_range) {
		super();

		m_primaryPeer = p_primaryPeer;
		m_range = p_range;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {

		final long primaryAndBackupPeers = p_importer.readLong();

		m_primaryPeer = (short) primaryAndBackupPeers;
		m_range = new long[] {p_importer.readLong(), p_importer.readLong()};

		return 2 * Long.BYTES + Short.BYTES;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		p_exporter.writeShort(getPrimaryPeer());
		p_exporter.writeLong(getStartID());
		p_exporter.writeLong(getEndID());

		return 2 * Long.BYTES + Short.BYTES;
	}

	@Override
	public int sizeofObject() {
		return 2 * Long.BYTES + Short.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}

	// Getter
	/**
	 * Get primary peer
	 * @return the primary peer
	 */
	public short getPrimaryPeer() {
		return m_primaryPeer;
	}

	/**
	 * Get range
	 * @return the beginning and ending of range
	 */
	public long[] getRange() {
		return m_range;
	}

	/**
	 * Get the start LocalID
	 * @return the start LocalID
	 */
	public long getStartID() {
		return m_range[0];
	}

	/**
	 * Get the end LocalID
	 * @return the end LocalID
	 */
	public long getEndID() {
		return m_range[1];
	}

	// Setter
	/**
	 * Set primary peer
	 * @param p_primaryPeer
	 *            the primary peer
	 */
	public void setPrimaryPeer(final short p_primaryPeer) {
		m_primaryPeer = p_primaryPeer;
	}

	// Methods
	/**
	 * Prints the locations
	 * @return String interpretation of locations
	 */
	@Override
	public String toString() {
		String ret;

		ret = m_primaryPeer + "";
		if (null != m_range) {
			ret += ", (" + m_range[0] + ", " + m_range[1] + ")";
		}
		return ret;
	}
}
