package de.uniduesseldorf.dxram.core.lookup;

import de.uniduesseldorf.utils.serialization.Exportable;
import de.uniduesseldorf.utils.serialization.Exporter;
import de.uniduesseldorf.utils.serialization.Importable;
import de.uniduesseldorf.utils.serialization.Importer;

/**
 * Stores locations
 * @author Kevin Beineke
 *         03.09.2013
 */
public final class Locations implements Importable, Exportable {

	// Attributes
	private short m_primaryPeer;
	private short[] m_backupPeers;
	private long[] m_range;

	/**
	 * Default constructor
	 */
	public Locations() {
		m_primaryPeer = -1;
		m_backupPeers = null;
		m_range = null;
	}
	
	// Constructors
	/**
	 * Creates an instance of Locations
	 * @param p_primaryPeer
	 *            the primary peer
	 * @param p_backupPeers
	 *            the backup peers
	 * @param p_range
	 *            the range's beginning and ending
	 */
	public Locations(final short p_primaryPeer, final short[] p_backupPeers, final long[] p_range) {
		super();

		m_primaryPeer = p_primaryPeer;
		m_backupPeers = p_backupPeers;
		m_range = p_range;
	}

	/**
	 * Creates an instance of Locations
	 * @param p_primaryAndBackupPeers
	 *            the locations in long representation
	 */
	public Locations(final long p_primaryAndBackupPeers) {
		this(p_primaryAndBackupPeers, null);
	}

	/**
	 * Creates an instance of Locations
	 * @param p_primaryAndBackupPeers
	 *            the locations in long representation
	 * @param p_range
	 *            the corresponding range
	 */
	public Locations(final long p_primaryAndBackupPeers, final long[] p_range) {
		this((short) p_primaryAndBackupPeers, new short[] {(short) ((p_primaryAndBackupPeers & 0x00000000FFFF0000L) >> 16),
				(short) ((p_primaryAndBackupPeers & 0x0000FFFF00000000L) >> 32), (short) ((p_primaryAndBackupPeers & 0xFFFF000000000000L) >> 48)}, p_range);
	}
	
	@Override
	public int importObject(Importer p_importer, int p_size) {
		
		long primaryAndBackupPeers = p_importer.readLong();
		
		m_primaryPeer = (short) primaryAndBackupPeers;
		m_backupPeers = new short[] {(short) ((primaryAndBackupPeers & 0x00000000FFFF0000L) >> 16),
				(short) ((primaryAndBackupPeers & 0x0000FFFF00000000L) >> 32), (short) ((primaryAndBackupPeers & 0xFFFF000000000000L) >> 48)};
		m_range = new long[] {p_importer.readLong(), p_importer.readLong()};
		
		return 3 * Long.BYTES;
	}
	
	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		p_exporter.writeLong(convertToLong());
		p_exporter.writeLong(getStartID());
		p_exporter.writeLong(getEndID());
		
		return 3 * Long.BYTES;
	}
	
	@Override
	public int sizeofObject() {
		return 3 * Long.BYTES;
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
	 * Get backup peers
	 * @return the backup peers
	 */
	public short[] getBackupPeers() {
		return m_backupPeers;
	}

	/**
	 * Get backup peers as long
	 * @return the backup peers
	 */
	public long getBackupPeersAsLong() {
		long ret = -1;
		if (null != m_backupPeers) {
			if (m_backupPeers.length == 3) {
				ret =
						((m_backupPeers[2] & 0x000000000000FFFFL) << 32) + ((m_backupPeers[1] & 0x000000000000FFFFL) << 16)
								+ (m_backupPeers[0] & 0x000000000000FFFFL);
			} else if (m_backupPeers.length == 2) {
				ret = ((-1 & 0x000000000000FFFFL) << 32) + ((m_backupPeers[1] & 0x000000000000FFFFL) << 16) + (m_backupPeers[0] & 0x000000000000FFFFL);
			} else {
				ret = ((-1 & 0x000000000000FFFFL) << 32) + ((-1 & 0x000000000000FFFFL) << 16) + (m_backupPeers[0] & 0x000000000000FFFFL);
			}
		}

		return ret;
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

	/**
	 * Set backup peers
	 * @param p_backupPeers
	 *            the backup peers
	 */
	public void setBackupPeers(final short[] p_backupPeers) {
		m_backupPeers = p_backupPeers;
	}

	// Methods
	/**
	 * Convert this instance to long
	 * @return the long representation
	 */
	public long convertToLong() {
		long ret;
		if (null != m_backupPeers) {
			if (m_backupPeers.length == 3) {
				ret =
						((m_backupPeers[2] & 0x000000000000FFFFL) << 48) + ((m_backupPeers[1] & 0x000000000000FFFFL) << 32)
								+ ((m_backupPeers[0] & 0x000000000000FFFFL) << 16) + (m_primaryPeer & 0x0000FFFF);
			} else if (m_backupPeers.length == 2) {
				ret = ((m_backupPeers[1] & 0x000000000000FFFFL) << 32) + ((m_backupPeers[0] & 0x000000000000FFFFL) << 16) + (m_primaryPeer & 0x0000FFFF);
			} else {
				ret = ((m_backupPeers[0] & 0x000000000000FFFFL) << 16) + (m_primaryPeer & 0x0000FFFF);
			}
		} else {
			ret = m_primaryPeer & 0x0000FFFF;
		}
		return ret;
	}

	/**
	 * Prints the locations
	 * @return String interpretation of locations
	 */
	@Override
	public String toString() {
		String ret;

		if (null != m_backupPeers) {
			if (m_backupPeers.length == 3) {
				if (m_backupPeers[0] == -1) {
					ret = m_primaryPeer + ", backup peers unknown (ask " + m_primaryPeer + ")";
				} else {
					ret = m_primaryPeer + ", [" + m_backupPeers[0] + ", " + m_backupPeers[1] + ", " + m_backupPeers[2] + "]";
				}
			} else if (m_backupPeers.length == 2) {
				ret = m_primaryPeer + ", [" + m_backupPeers[0] + ", " + m_backupPeers[1] + "]";
			} else {
				ret = m_primaryPeer + ", [" + m_backupPeers[0] + "]";
			}
		} else {
			ret = m_primaryPeer + ", no backup peers";
		}
		if (null != m_range) {
			ret += ", (" + m_range[0] + ", " + m_range[1] + ")";
		}
		return ret;
	}
}