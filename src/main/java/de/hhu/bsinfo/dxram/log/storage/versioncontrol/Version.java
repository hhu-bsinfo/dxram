/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.versioncontrol;

/**
 * Class for bundling the epoch and version of a log entry.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2016
 */
public final class Version {

    static final int INVALID_VERSION = -1;

    private final short m_epoch;
    private final int m_version;

    /**
     * Creates an instance of Version.
     *
     * @param p_epoch
     *         the epoch
     * @param p_version
     *         the version
     */
    public Version(final short p_epoch, final int p_version) {
        m_epoch = p_epoch;
        m_version = p_version;
    }

    /**
     * Returns the version.
     *
     * @return the version
     */
    public int getVersion() {
        return m_version;
    }

    /**
     * Returns the epoch.
     *
     * @return the epoch
     */
    public short getEpoch() {
        return m_epoch;
    }

    /**
     * Returns the eon.
     *
     * @return the eon
     */
    public byte getEon() {
        return (byte) (m_epoch >> 15);
    }

    /**
     * Compares two Versions; Is faster than equals as class type is known.
     *
     * @param p_cmp
     *         the EpochVersion to compare with
     * @return true if version, epoch and eon is equal
     */
    public boolean isEqual(final Version p_cmp) {
        return m_epoch == p_cmp.m_epoch && m_version == p_cmp.m_version;
    }

    /**
     * Compares two Versions.
     *
     * @param p_cmp
     *         the EpochVersion to compare with
     * @return true if version, epoch and eon is equal
     */
    @Override
    public boolean equals(final Object p_cmp) {
        return p_cmp instanceof Version && m_epoch == ((Version) p_cmp).m_epoch &&
                m_version == ((Version) p_cmp).m_version;
    }

    @Override
    public int hashCode() {
        int ret = 97546154;

        ret = 37 * ret + m_epoch;
        ret = 37 * ret + m_version;

        return ret;
    }

    @Override
    public String toString() {
        return m_epoch + "-" + m_version;
    }
}
