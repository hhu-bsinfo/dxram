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

package de.hhu.bsinfo.dxram.log.storage;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;

/**
 * Class to bundle versions for secondary logs
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 24.11.2016
 */
public final class TemporaryVersionsStorage {

    private long m_maximumBackupRangeSize;
    private VersionsArray m_versionsArray;
    private VersionsHashTable m_versionsHashTable;

    // Constructors

    /**
     * Creates an instance of TemporaryVersionsStorage
     *
     * @param p_secondaryLogSize
     *         the size of the secondary log
     */
    public TemporaryVersionsStorage(final long p_secondaryLogSize) {
        m_maximumBackupRangeSize = p_secondaryLogSize / 2;

        // Initialize array with default value suitable for 64-byte chunks; use localID 0 to fit first backup range
        // as well; size: ~28 MB
        m_versionsArray = new VersionsArray(
                AbstractSecLogEntryHeader.getMaximumNumberOfVersions(m_maximumBackupRangeSize, 64, false));

        // Initialize hashtable with default value suitable for 64-byte chunks; size: ~54 MB
        // (we do not want to rehash often, might still be increased)
        m_versionsHashTable = new VersionsHashTable(
                AbstractSecLogEntryHeader.getMaximumNumberOfVersions(m_maximumBackupRangeSize, 64, true));
    }

    // Methods

    /**
     * Clears the versions data structures
     */
    public void clear() {
        m_versionsArray.clear();
        m_versionsHashTable.clear();
    }

    /**
     * Returns the current version for given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the version
     */
    Version get(final long p_chunkID) {
        return m_versionsHashTable.get(p_chunkID);
    }

    /**
     * Returns the current version for given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_lowestLID
     *         the lowest localID
     * @return the version
     */
    Version get(final long p_chunkID, final long p_lowestLID) {
        if (p_chunkID >= p_lowestLID && p_chunkID < p_lowestLID + m_versionsArray.capacity()) {
            return m_versionsArray.get(p_chunkID, p_lowestLID);
        } else {
            return m_versionsHashTable.get(p_chunkID);
        }
    }

    /**
     * Returns the current version for given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_lowestCID
     *         the lowest CID
     * @return the version
     */
    Version get(final long p_chunkID, final long p_lowestCID, final SecondaryLog.Statistics p_stat) {
        long localID = ChunkID.getLocalID(p_chunkID);
        long lowestLID = ChunkID.getLocalID(p_lowestCID);
        if (localID >= lowestLID && localID < lowestLID + m_versionsArray.capacity()) {
            p_stat.m_readVersionsFromArray++;
            return m_versionsArray.get(p_chunkID, p_lowestCID);
        } else {
            p_stat.m_readVersionsFromHashTable++;
            return m_versionsHashTable.get(p_chunkID);
        }
    }

    /**
     * Returns the versions array for storing versions of normal log entries (created sequentially)
     *
     * @return the versions array
     */
    VersionsArray getVersionsArray() {
        return m_versionsArray;
    }

    /**
     * Returns the hashtable for storing versions of migration/recovered log entries
     *
     * @return the hashtable
     */
    VersionsHashTable getVersionsHashTable() {
        return m_versionsHashTable;
    }
}
