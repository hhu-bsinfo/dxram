
package de.hhu.bsinfo.dxram.backup;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Stores a backup range (for chunk and backup service)
 * @author Kevin Beineke 10.06.2015
 */
public final class BackupRange implements Importable, Exportable {

	// Attributes
	private long m_firstChunkIDORRangeID = -1;
	private short[] m_backupPeers;

	// Constructors

	/**
	 * Creates an instance of BackupRange
	 */
	public BackupRange() {}

	/**
	 * Creates an instance of BackupRange
	 * @param p_firstChunkIDORRangeID
	 *            the RangeID or the first ChunkID
	 * @param p_backupPeers
	 *            the backup peers
	 */
	public BackupRange(final long p_firstChunkIDORRangeID, final short[] p_backupPeers) {
		super();

		m_firstChunkIDORRangeID = p_firstChunkIDORRangeID;
		m_backupPeers = p_backupPeers;
	}

	/**
	 * Creates an instance of BackupRange
	 * @param p_firstChunkIDORRangeID
	 *            the RangeID or the first ChunkID
	 * @param p_backupPeers
	 *            the backup range in long representation
	 */
	public BackupRange(final long p_firstChunkIDORRangeID, final long p_backupPeers) {
		this(p_firstChunkIDORRangeID, BackupRange.convert(p_backupPeers));
	}

	/**
	 * Converts backup peers from long to short[]
	 * @param p_backupPeers
	 *            the backup peers in long representation
	 * @return the backup peers in short[] representation
	 */
	public static short[] convert(final long p_backupPeers) {
		return new short[] {(short) (p_backupPeers & 0x000000000000FFFFL),
				(short) ((p_backupPeers & 0x00000000FFFF0000L) >> 16),
				(short) ((p_backupPeers & 0x0000FFFF00000000L) >> 32)};
	}

	/**
	 * Converts backup peers from short[] to long
	 * @param p_backupPeers
	 *            the backup peers in short[] representation
	 * @return the backup peers in long representation
	 */
	public static long convert(final short[] p_backupPeers) {
		long ret = -1;
		if (null != p_backupPeers) {
			if (p_backupPeers.length == 3) {
				ret = ((p_backupPeers[2] & 0x000000000000FFFFL) << 32)
						+ ((p_backupPeers[1] & 0x000000000000FFFFL) << 16)
						+ (p_backupPeers[0] & 0x000000000000FFFFL);
			} else if (p_backupPeers.length == 2) {
				ret = ((-1 & 0x000000000000FFFFL) << 32) + ((p_backupPeers[1] & 0x000000000000FFFFL) << 16)
						+ (p_backupPeers[0] & 0x000000000000FFFFL);
			} else {
				ret = ((-1 & 0x000000000000FFFFL) << 32) + ((-1 & 0x000000000000FFFFL) << 16) + (p_backupPeers[0]
						& 0x000000000000FFFFL);
			}
		}

		return ret;
	}

	/**
	 * Replaces the failed backup peer
	 * @param p_newPeer
	 *            the failed backup peer
	 * @param p_newPeer
	 *            the new backup peer
	 * @return all backup peers in a short array
	 */
	public static long replaceBackupPeer(final long p_backupPeers, final short p_failedPeer, final short p_newPeer) {
		long backupPeers = p_backupPeers;
		short nextBackupPeer;
		int lastPos;

		for (int i = 0; i < 3; i++) {
			if (p_failedPeer == (short) ((backupPeers & (0xFFFF << (i * 16))) >> (i * 16))) {
				for (lastPos = i; lastPos < 2; lastPos++) {
					nextBackupPeer = (short) ((backupPeers & (0xFFFF << ((lastPos + 1) * 16))) >> ((lastPos + 1) * 16));
					if (nextBackupPeer == -1) {
						// Break if backups are incomplete
						break;
					}
					replace(backupPeers, lastPos, nextBackupPeer);
				}
				replace(backupPeers, lastPos + 1, p_newPeer);
				break;
			}
		}

		return backupPeers;
	}

	/**
	 * Replaces the backup peer at given index
	 * @param p_backupPeers
	 *            all backup peers in long representation
	 * @param p_index
	 *            the index
	 * @param p_newPeer
	 *            the replacement
	 * @return all backup peers including replacement
	 */
	private static long replace(final long p_backupPeers, final int p_index, final short p_newPeer) {
		return p_backupPeers & ~(0xFFFF << (p_index * 16)) + ((long) p_newPeer << (p_index * 16));
	}

	// Getter

	/**
	 * Returns RangeID or first ChunkID
	 * @return RangeID or first ChunkID
	 */
	public long getRangeID() {
		return m_firstChunkIDORRangeID;
	}

	/**
	 * Get backup peers
	 * @return the backup peers
	 */
	public short[] getBackupPeers() {
		return m_backupPeers;
	}

	/**
	 * Get backup peers
	 * @return the backup peers
	 */
	public short[] getCopyOfBackupPeers() {
		return Arrays.copyOf(m_backupPeers, m_backupPeers.length);
	}

	/**
	 * Get backup peers as long
	 * @return the backup peers
	 */
	public long getBackupPeersAsLong() {
		return BackupRange.convert(m_backupPeers);
	}

	// Methods
	/**
	 * Replaces the failed backup peer
	 * @param p_newPeer
	 *            the failed backup peer
	 * @param p_newPeer
	 *            the new backup peer
	 * @return all backup peers in a short array
	 */
	public short[] replaceBackupPeer(final short p_failedPeer, final short p_newPeer) {
		short[] ret = null;

		for (int i = 0; i < m_backupPeers.length; i++) {
			if (m_backupPeers[i] == p_failedPeer) {
				ret = replaceBackupPeer(i, p_newPeer);
				break;
			}
		}

		return ret;
	}

	/**
	 * Replaces the failed backup peer at given index
	 * @param p_index
	 *            the index
	 * @param p_newPeer
	 *            the new backup peer
	 * @return all backup peers in a short array
	 */
	public short[] replaceBackupPeer(final int p_index, final short p_newPeer) {
		int lastPos;

		for (lastPos = p_index; lastPos < m_backupPeers.length - 1; lastPos++) {
			if (m_backupPeers[lastPos + 1] == -1) {
				// Break if backups are incomplete
				break;
			}
			m_backupPeers[lastPos] = m_backupPeers[lastPos + 1];
		}
		m_backupPeers[lastPos + 1] = p_newPeer;

		return m_backupPeers;
	}

	/**
	 * Prints the backup range
	 * @return String interpretation of BackupRange
	 */
	@Override
	public String toString() {
		String ret;

		ret = "" + ChunkID.toHexString(m_firstChunkIDORRangeID);
		if (null != m_backupPeers) {
			if (m_backupPeers.length == 3) {
				ret = "[" + NodeID.toHexString(m_backupPeers[0]) + ", " + NodeID.toHexString(m_backupPeers[1])
						+ ", " + NodeID.toHexString(m_backupPeers[2]) + "]";
			} else if (m_backupPeers.length == 2) {
				ret = "[" + NodeID.toHexString(m_backupPeers[0]) + ", " + NodeID.toHexString(m_backupPeers[1]) + "]";
			} else {
				ret = "[" + NodeID.toHexString(m_backupPeers[0]) + "]";
			}
		} else {
			ret = "no backup peers";
		}

		return ret;
	}

	@Override
	public void importObject(final Importer p_importer) {
		long backupPeers = -1;

		m_firstChunkIDORRangeID = p_importer.readLong();
		backupPeers = p_importer.readLong();
		m_backupPeers = BackupRange.convert(backupPeers);
	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		p_exporter.writeLong(getRangeID());
		p_exporter.writeLong(getBackupPeersAsLong());
	}

	@Override
	public int sizeofObject() {
		return sizeofObjectStatic();
	}

	/**
	 * The size of all attributes
	 * @return the size
	 */
	public static int sizeofObjectStatic() {
		return 2 * Long.BYTES;
	}
}
