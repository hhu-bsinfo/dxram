package de.hhu.bsinfo.dxram.log.storage;

/**
 * Class for bundling the epoch and version of a log entry
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2016
 */
public class Version {

    private short m_epoch;
    private int m_version;

    /**
     * Creates an instance of EpochVersion
     *
     * @param p_epoch
     *     the epoch
     * @param p_version
     *     the version
     */
    public Version(final short p_epoch, final int p_version) {
        m_epoch = p_epoch;
        m_version = p_version;
    }

    /**
     * Returns the epoch
     *
     * @return the epoch
     */
    public short getEpoch() {
        return m_epoch;
    }

    /**
     * Returns the eon
     *
     * @return the eon
     */
    public byte getEon() {
        return (byte) (m_epoch >> 15);
    }

    /**
     * Returns the version
     *
     * @return the version
     */
    public int getVersion() {
        return m_version;
    }

    /**
     * Compares two EpochVersions
     *
     * @param p_cmp
     *     the EpochVersion to compare with
     * @return true if version, epoch and eon is equal
     */
    @Override
    public boolean equals(final Object p_cmp) {
        return p_cmp instanceof Version && m_epoch == ((Version) p_cmp).m_epoch && m_version == ((Version) p_cmp).m_version;
    }

    @Override
    public int hashCode() {
        int ret = 97546154;

        ret = 37 * ret + m_epoch;
        ret = 37 * ret + m_version;

        return ret;
    }

    /**
     * Compares two EpochVersions
     *
     * @param p_cmp
     *     the EpochVersion to compare with
     * @return 1 if p_cmp is smaller, 0 if they are equal and -1 if p_cmp is greater
     */
    public int compareTo(final Version p_cmp) {
        int ret;

        if (m_epoch == p_cmp.m_epoch) {
            if (m_version == p_cmp.m_version) {
                ret = 0;
            } else if (m_version < p_cmp.m_version) {
                ret = -1;
            } else {
                ret = 1;
            }
        } else {
            if (m_epoch > Short.MAX_VALUE - 100 && p_cmp.m_epoch < 100) {
                // Epoch overflow
                ret = -1;
            } else if (m_epoch < 100 && p_cmp.m_epoch > Short.MAX_VALUE - 100) {
                // Epoch overflow
                ret = 1;
            } else {
                if (m_epoch < p_cmp.m_epoch) {
                    ret = -1;
                } else {
                    ret = 1;
                }
            }
        }

        return ret;
    }
}
