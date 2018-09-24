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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxutils.hashtable.LongHashTable;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Class to bundle versions for secondary logs.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 24.11.2016
 */
public final class TemporaryVersionStorage {

    private static final Logger LOGGER = LogManager.getFormatterLogger(TemporaryVersionStorage.class.getSimpleName());

    private static final ValuePool SOP_READ_VERSIONS_FROM_ARRAY =
            new ValuePool(TemporaryVersionStorage.class, "ReadVersionsFromArray");
    private static final ValuePool SOP_READ_VERSIONS_FROM_HASH_TABLE =
            new ValuePool(TemporaryVersionStorage.class, "ReadVersionsFromHashTable");

    static {
        StatisticsManager.get().registerOperation(TemporaryVersionStorage.class, SOP_READ_VERSIONS_FROM_ARRAY);
        StatisticsManager.get().registerOperation(TemporaryVersionStorage.class, SOP_READ_VERSIONS_FROM_HASH_TABLE);
    }

    private final long m_maximumBackupRangeSize;
    private final VersionArray m_versionArray;
    private final LongHashTable m_versionHashTable;

    /**
     * Creates an instance of TemporaryVersionStorage.
     *
     * @param p_secondaryLogSize
     *         the size of the secondary log
     */
    public TemporaryVersionStorage(final long p_secondaryLogSize) {
        m_maximumBackupRangeSize = p_secondaryLogSize / 2;

        // Initialize array with default value suitable for 64-byte chunks; use localID 0 to fit first backup range
        // as well; size: ~28 MB
        m_versionArray = new VersionArray(
                AbstractSecLogEntryHeader.getMaximumNumberOfVersions(m_maximumBackupRangeSize, 64, false));

        // Initialize hashtable with default value suitable for 64-byte chunks; size: ~54 MB
        // (we do not want to rehash often, might still be increased)
        m_versionHashTable = new LongHashTable(
                AbstractSecLogEntryHeader.getMaximumNumberOfVersions(m_maximumBackupRangeSize, 64, true));
    }

    /**
     * Clears the version data structures.
     */
    public void clear() {
        m_versionArray.clear();
        m_versionHashTable.clear();
    }

    /**
     * Returns the current version for given ChunkID.
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the version
     */
    public Version get(final long p_chunkID) {
        long value = m_versionHashTable.get(p_chunkID + 1);
        if (value != -1) {
            return new Version((short) (value >> 32), (int) value);
        }
        return null;
    }

    /**
     * Returns the current version for given ChunkID.
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_lowestCID
     *         the lowest CID
     * @return the version
     */
    public Version get(final long p_chunkID, final long p_lowestCID) {
        long localID = ChunkID.getLocalID(p_chunkID);
        long lowestLID = ChunkID.getLocalID(p_lowestCID);
        if (localID >= lowestLID && localID < lowestLID + m_versionArray.capacity()) {
            SOP_READ_VERSIONS_FROM_ARRAY.inc();
            return m_versionArray.get(p_chunkID, p_lowestCID);
        } else {
            SOP_READ_VERSIONS_FROM_HASH_TABLE.inc();
            return get(p_chunkID);
        }
    }

    /**
     * Returns the versions array for storing versions of normal log entries (created sequentially).
     *
     * @return the versions array
     */
    VersionArray getVersionsArray() {
        return m_versionArray;
    }

    /**
     * Returns the hashtable for storing versions of migration/recovered log entries.
     *
     * @return the hashtable
     */
    LongHashTable getVersionsHashTable() {
        return m_versionHashTable;
    }
}
