
package de.hhu.bsinfo.dxram.log;

/**
 * Class for bundling the epoch and version of a log entry
 * @author Kevin Beineke
 */
public class EpochVersion {
	private short m_epoch;
	private int m_version;

	/**
	 * Creates an instance of EpochVersion
	 * @param p_epoch
	 *            the epoch
	 * @param p_version
	 *            the version
	 */
	public EpochVersion(final short p_epoch, final int p_version) {
		m_epoch = p_epoch;
		m_version = p_version;
	}

	/**
	 * Returns the epoch
	 * @return the epoch
	 */
	public short getEpoch() {
		return m_epoch;
	}

	/**
	 * Returns the version
	 * @return the version
	 */
	public int getVersion() {
		return m_version;
	}

	public boolean equals(final EpochVersion p_cmp) {
		return (m_epoch == p_cmp.getEpoch() && m_version == p_cmp.getVersion());
	}
}
