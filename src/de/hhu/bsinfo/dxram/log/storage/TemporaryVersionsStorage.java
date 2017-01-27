/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;

/**
 * Class to bundle versions for normal secondary logs and migration secondary logs
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 24.11.2016
 */
public final class TemporaryVersionsStorage {

    private long m_secondaryLogSize;
    private int[] m_versionsForNormalLog;
    private VersionsHashTable m_versionsForMigrationLog;

    // Constructors

    /**
     * Creates an instance of TemporaryVersionsStorage
     *
     * @param p_secondaryLogSize
     *     the size of the secondary log
     */
    public TemporaryVersionsStorage(final long p_secondaryLogSize) {
        m_secondaryLogSize = p_secondaryLogSize;
    }

    /**
     * Creates an instance of TemporaryVersionsStorage
     *
     * @param p_secondaryLogSize
     *     the size of the secondary log
     * @param p_size
     *     the size of the int-array
     */
    TemporaryVersionsStorage(final long p_secondaryLogSize, final int p_size) {
        m_secondaryLogSize = p_secondaryLogSize;

        m_versionsForNormalLog = new int[p_size];
        Arrays.fill(m_versionsForNormalLog, -1);
    }

    // Methods

    /**
     * Returns the int-array for storing versions of normal logs
     *
     * @return the int-array
     */
    int[] getVersionsForNormalLog() {
        if (m_versionsForNormalLog == null) {
            // Initialize array with default value suitable for 128-byte chunks (might be increased in VersionBuffer)
            m_versionsForNormalLog = new int[(int) (2 * m_secondaryLogSize / (128 + AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(false, 128)))];
            Arrays.fill(m_versionsForNormalLog, -1);
        }
        return m_versionsForNormalLog;
    }

    /**
     * Returns the hashtable for storing versions of migration logs
     *
     * @return the hashtable
     */
    VersionsHashTable getVersionsForMigrationLog() {
        if (m_versionsForMigrationLog == null) {
            // Initialize hashtable with default value suitable for 32-byte chunks
            // (we do not want to rehash often, might still be increased in VersionBuffer)
            m_versionsForMigrationLog = new VersionsHashTable((int) (m_secondaryLogSize / (32 + AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(true, 32))));
        }
        return m_versionsForMigrationLog;
    }

    /**
     * Clears the versions data structure
     *
     * @param p_logStoresMigrations
     *     whether to clear the int-array (false) or hashtable (true)
     */
    public void clear(final boolean p_logStoresMigrations) {
        if (!p_logStoresMigrations) {
            Arrays.fill(m_versionsForNormalLog, -1);
        } else {
            m_versionsForMigrationLog.clear();
        }
    }

    /**
     * Resizes the int-array
     *
     * @param p_size
     *     the new size
     * @return the new int-array
     */
    int[] resizeVersionsForNormalLog(final int p_size) {
        m_versionsForNormalLog = new int[p_size];
        Arrays.fill(m_versionsForNormalLog, -1);

        return m_versionsForNormalLog;
    }

    /**
     * Resizes the hashtable
     *
     * @param p_size
     *     the new size
     * @return the new hashtable
     */
    VersionsHashTable resizeVersionsForMigrationLog(final int p_size) {
        m_versionsForMigrationLog = new VersionsHashTable(p_size);

        return m_versionsForMigrationLog;
    }
}
