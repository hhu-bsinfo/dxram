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

package de.hhu.bsinfo.dxram.mem;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.soh.SmallObjectHeap;

/**
 * Paging-like Tables for the ChunkID-VA mapping
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class CIDTable {

    private static final byte ENTRY_SIZE = 5;
    static final byte LID_TABLE_LEVELS = 4;
    static final long BITMASK_ADDRESS = 0x7FFFFFFFFFL;
    private static final long BIT_FLAG = 0x8000000000L;
    private static final long FULL_FLAG = BIT_FLAG;
    private static final long DELETED_FLAG = BIT_FLAG;
    private static final Logger LOGGER = LogManager.getFormatterLogger(CIDTable.class.getSimpleName());
    // statistics recorder
    private static final StatisticsOperation SOP_CREATE_NID_TABLE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "CreateNIDTable");
    private static final StatisticsOperation SOP_CREATE_LID_TABLE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "CreateLIDTable");
    private static final byte BITS_PER_LID_LEVEL = 48 / LID_TABLE_LEVELS;
    static final int ENTRIES_PER_LID_LEVEL = (int) Math.pow(2.0, BITS_PER_LID_LEVEL);
    private static final int LID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_LID_LEVEL + 7;
    private static final long LID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_PER_LID_LEVEL) - 1;
    private static final byte BITS_FOR_NID_LEVEL = 16;
    static final int ENTRIES_FOR_NID_LEVEL = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL);
    private static final int NID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_FOR_NID_LEVEL + 7;
    private static final long NID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL) - 1;
    private GetNodeIdHook m_nodeIdHook;
    private long m_addressTableDirectory = -1;
    private SmallObjectHeap m_rawMemory;
    private int m_tableCount = -1;
    private long m_totalMemoryTables = -1;

    private LIDStore m_store;
    private long m_nextLocalID;

    private TranslationCache m_cache;

    /**
     * Creates an instance of CIDTable
     *
     * @param p_nodeIdHook
     *     Instance of a class implementing the node id hook.
     */
    public CIDTable(final GetNodeIdHook p_nodeIdHook) {
        m_nodeIdHook = p_nodeIdHook;
    }

    /**
     * Get the number of tables currently allocated.
     *
     * @return Number of tables currently allocated.
     */
    int getTableCount() {
        return m_tableCount;
    }

    /**
     * Get the total amount of memory used by the tables.
     *
     * @return Amount of memory used by the tables (in bytes)
     */
    long getTotalMemoryTables() {
        return m_totalMemoryTables;
    }

    /**
     * Get a free LID from the CIDTable
     *
     * @return a free LID and version, or -1 if there is none
     */
    long getFreeLID() {
        long ret = -1;

        if (m_store != null) {
            ret = m_store.get();
        }

        // If no free ID exist, get next local ID
        if (ret == -1) {
            ret = m_nextLocalID++;
        }

        return ret;
    }

    /**
     * Returns the ChunkID ranges of all locally stored Chunks
     *
     * @return the ChunkID ranges in an ArrayList
     */
    ArrayList<Long> getCIDRangesOfAllLocalChunks() {
        ArrayList<Long> ret = null;
        long entry;
        long intervalStart;
        long intervalEnd;

        if (m_store != null) {
            ret = new ArrayList<Long>();
            for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
                entry = readEntry(m_addressTableDirectory, i) & BITMASK_ADDRESS;
                if (entry > 0) {
                    if (i == (m_nodeIdHook.getNodeId() & 0xFFFF)) {
                        ret.addAll(
                            getAllRanges((long) i << 48, readEntry(m_addressTableDirectory, i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1));
                    }
                }
            }
        }

        // compress intervals
        assert ret != null;
        if (ret.size() >= 2) {
            if (ret.size() % 2 != 0) {
                // throw new MemoryException("internal error in getChunkIDRangesOfAllChunks");
                // System.out.println("error: in ChunkIDRange list");
            } else {
                for (int i = 0; i < ret.size() - 2; i += 2) {
                    intervalEnd = ChunkID.getLocalID(ret.get(i + 1));
                    intervalStart = ChunkID.getLocalID(ret.get(i + 2));

                    // can we merge intervals?
                    if (intervalEnd + 1 == intervalStart) {
                        // System.out.println(" remove el.");
                        ret.remove(i + 1);
                        ret.remove(i + 1);
                        i -= 2;
                    }
                }
            }
        }

        return ret;
    }

    /**
     * Returns the ChunkIDs of all migrated Chunks
     *
     * @return the ChunkIDs of all migrated Chunks
     */
    ArrayList<Long> getCIDOfAllMigratedChunks() {
        ArrayList<Long> ret = null;
        long entry;

        if (m_store != null) {
            ret = new ArrayList<Long>();
            for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
                entry = readEntry(m_addressTableDirectory, i) & BITMASK_ADDRESS;
                if (entry > 0 && i != (m_nodeIdHook.getNodeId() & 0xFFFF)) {
                    ret.addAll(
                        getAllEntries((long) i << 48, readEntry(m_addressTableDirectory, i & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1));
                }
            }
        }

        return ret;
    }

    /**
     * Initializes the CIDTable
     *
     * @param p_rawMemory
     *     The raw memory instance to use for allocation.
     */
    public void initialize(final SmallObjectHeap p_rawMemory) {
        m_rawMemory = p_rawMemory;
        m_tableCount = 0;
        m_totalMemoryTables = 0;
        m_addressTableDirectory = createNIDTable();

        m_store = new LIDStore();
        m_nextLocalID = 1;

        // NOTE: 10 seems to be a good value because it doesn't add too much overhead when creating huge ranges chunks
        // but still allows 10 * 4096 translations to be cached for fast lookup and gets/puts
        // (value determined by profiling the application)
        m_cache = new TranslationCache(10);

        // #if LOGGER >= INFO
        LOGGER.info("CIDTable: init success (page directory at: 0x%X)", m_addressTableDirectory);
        // #endif /* LOGGER >= INFO */
    }

    /**
     * Gets an entry of the level 0 table
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @return the entry. 0 for invalid/unused.
     */
    public long get(final long p_chunkID) {
        long index;
        long entry;

        int level = 0;
        long addressTable;
        boolean putCache = false;

        addressTable = m_cache.getTableLevel0(p_chunkID);
        if (addressTable == -1) {
            level = LID_TABLE_LEVELS;
            addressTable = m_addressTableDirectory;
            putCache = true;
        }

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            if (level > 0) {
                entry = readEntry(addressTable, index) & BITMASK_ADDRESS;

                if (entry <= 0) {
                    break;
                }

                // move on to next table
                addressTable = entry & BITMASK_ADDRESS;
            } else {
                if (putCache) {
                    m_cache.putTableLevel0(p_chunkID, addressTable);
                }

                return readEntry(addressTable, index) & BITMASK_ADDRESS;
            }

            level--;
        } while (level >= 0);

        return 0;
    }

    /**
     * Sets an entry of the level 0 table
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @param p_addressChunk
     *     the address of the chunk
     * @return True if successful, false if allocation of a new table failed, out of memory
     */
    public boolean set(final long p_chunkID, final long p_addressChunk) {
        long index;
        long entry;

        int level = 0;
        long addressTable;
        boolean putCache = false;

        addressTable = m_cache.getTableLevel0(p_chunkID);
        if (addressTable == -1) {
            level = LID_TABLE_LEVELS;
            addressTable = m_addressTableDirectory;
            putCache = true;
        }

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            if (level > 0) {
                // Read table entry
                entry = readEntry(addressTable, index);
                if (entry == 0) {
                    entry = createLIDTable();
                    if (entry == -1) {
                        return false;
                    }
                    writeEntry(addressTable, index, entry);
                }

                // move on to next table
                addressTable = entry & BITMASK_ADDRESS;
            } else {
                if (putCache) {
                    m_cache.putTableLevel0(p_chunkID, addressTable);
                }

                // Set the level 0 entry
                // valid and active entry, delete flag 0
                writeEntry(addressTable, index, p_addressChunk & BITMASK_ADDRESS);

                return true;
            }

            level--;
        } while (level >= 0);

        return true;
    }

    /**
     * Gets and deletes an entry of the level 0 table
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @param p_flagZombie
     *     Flag the deleted entry as a zombie or not zombie i.e. fully deleted.
     * @return The address of the chunk which was removed from the table.
     */
    public long delete(final long p_chunkID, final boolean p_flagZombie) {
        long ret = -1;
        long index;
        long entry;

        int level = LID_TABLE_LEVELS;
        long addressTable = m_addressTableDirectory;

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            if (level > 0) {
                // Read table entry
                entry = readEntry(addressTable, index);
                if ((entry & FULL_FLAG) > 0) {
                    // Delete full flag
                    entry &= ~FULL_FLAG;
                    writeEntry(addressTable, index, entry);
                }

                if ((entry & BITMASK_ADDRESS) == 0) {
                    break;
                }

                // Delete entry in the following table
                addressTable = entry & BITMASK_ADDRESS;
            } else {
                ret = readEntry(addressTable, index) & BITMASK_ADDRESS;

                // Delete the level 0 entry
                // invalid + active address but deleted flag 1
                // -> zombie entry
                if (p_flagZombie) {
                    writeEntry(addressTable, index, ret | DELETED_FLAG);
                } else {
                    // delete flag cleared, but address is 0 -> free entry
                    writeEntry(addressTable, index, 0);
                }
            }

            level--;
        } while (level >= 0);

        return ret;
    }

    /**
     * Prints debug informations
     */
    public void printDebugInfos() {
        StringBuilder infos;
        int[] count;

        count = new int[LID_TABLE_LEVELS + 1];

        countTables(m_addressTableDirectory, LID_TABLE_LEVELS, count);

        infos = new StringBuilder();
        infos.append("\nCIDTable:\n");
        for (int i = LID_TABLE_LEVELS; i >= 0; i--) {
            infos.append('\t');
            infos.append(count[i]);
            infos.append(" table(s) on level ");
            infos.append(i);
            infos.append('\n');
        }

        System.out.println(infos);
    }

    /**
     * Disengages the CIDTable
     */
    void disengage() {
        m_store = null;

        m_addressTableDirectory = -1;
    }

    // -----------------------------------------------------------------------------------------

    /**
     * Puts the LocalID of a deleted migrated Chunk to LIDStore
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @return m_cidTable
     */
    boolean putChunkIDForReuse(final long p_chunkID) {
        return m_store.put(ChunkID.getLocalID(p_chunkID));
    }

    /**
     * Reads a table entry
     *
     * @param p_addressTable
     *     the table
     * @param p_index
     *     the index of the entry
     * @return the entry
     */
    long readEntry(final long p_addressTable, final long p_index) {
        return m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFFFFFFL;
    }

    /**
     * Writes a table entry
     *
     * @param p_addressTable
     *     the table
     * @param p_index
     *     the index of the entry
     * @param p_entry
     *     the entry
     */
    void writeEntry(final long p_addressTable, final long p_index, final long p_entry) {
        long value;

        value = m_rawMemory.readLong(p_addressTable, ENTRY_SIZE * p_index) & 0xFFFFFF0000000000L;
        value += p_entry & 0xFFFFFFFFFFL;

        m_rawMemory.writeLong(p_addressTable, ENTRY_SIZE * p_index, value);
    }

    /**
     * Get the address of the table directory
     *
     * @return Address of table directory
     */
    long getAddressTableDirectory() {
        return m_addressTableDirectory;
    }

    /**
     * Creates the NodeID table
     *
     * @return the address of the table
     */
    private long createNIDTable() {
        long ret;

        // #ifdef STATISTICS
        SOP_CREATE_NID_TABLE.enter(NID_TABLE_SIZE);

        MemoryManagerComponent.SOP_MALLOC.enter(NID_TABLE_SIZE);
        // #endif /* STATISTICS */
        ret = m_rawMemory.malloc(NID_TABLE_SIZE);
        // #ifdef STATISTICS
        MemoryManagerComponent.SOP_MALLOC.leave();
        // #endif /* STATISTICS */
        if (ret != -1) {
            m_rawMemory.set(ret, NID_TABLE_SIZE, (byte) 0);
            m_totalMemoryTables += NID_TABLE_SIZE;
            m_tableCount++;
        }
        // #ifdef STATISTICS
        SOP_CREATE_NID_TABLE.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Creates a table
     *
     * @return the address of the table
     */
    private long createLIDTable() {
        long ret;

        // #ifdef STATISTICS
        SOP_CREATE_LID_TABLE.enter(LID_TABLE_SIZE);

        MemoryManagerComponent.SOP_MALLOC.enter(LID_TABLE_SIZE);
        // #endif /* STATISTICS */
        ret = m_rawMemory.malloc(LID_TABLE_SIZE);
        // #ifdef STATISTICS
        MemoryManagerComponent.SOP_MALLOC.leave();
        // #endif /* STATISTICS */
        if (ret != -1) {
            m_rawMemory.set(ret, LID_TABLE_SIZE, (byte) 0);
            m_totalMemoryTables += LID_TABLE_SIZE;
            m_tableCount++;
        }
        // #ifdef STATISTICS
        SOP_CREATE_LID_TABLE.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Adds all ChunkID ranges to an ArrayList
     *
     * @param p_unfinishedCID
     *     the unfinished ChunkID
     * @param p_table
     *     the current table
     * @param p_level
     *     the current table level
     * @return the ArrayList
     */
    private ArrayList<Long> getAllRanges(final long p_unfinishedCID, final long p_table, final int p_level) {
        ArrayList<Long> ret;
        long entry;
        int range = 0;

        ret = new ArrayList<Long>();
        for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
            entry = readEntry(p_table, i);
            if (entry > 0) {

                if ((entry & DELETED_FLAG) == 0) {

                    if (p_level > 0) {
                        ret.addAll(getAllRanges(p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level), entry & BITMASK_ADDRESS, p_level - 1));
                    } else {
                        if (range == 0) {
                            range = 1;
                            ret.add(p_unfinishedCID + i);
                        } else {
                            ret.remove(ret.size() - 1);
                        }
                        ret.add(p_unfinishedCID + i);
                    }
                }
            } else {
                range = 0;
            }
        }

        return ret;
    }

    /**
     * Adds all ChunkIDs to an ArrayList
     *
     * @param p_unfinishedCID
     *     the unfinished ChunkID
     * @param p_table
     *     the current table
     * @param p_level
     *     the current table level
     * @return the ArrayList
     */
    private ArrayList<Long> getAllEntries(final long p_unfinishedCID, final long p_table, final int p_level) {
        ArrayList<Long> ret;
        long entry;

        // System.out.println("Entering with " + ChunkID.toHexString(p_unfinishedCID));

        ret = new ArrayList<Long>();
        for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
            entry = readEntry(p_table, i);
            if (entry > 0) {
                if (p_level > 0) {
                    ret.addAll(getAllEntries(p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level), entry & BITMASK_ADDRESS, p_level - 1));
                } else {
                    ret.add(p_unfinishedCID + i);
                }
            }
        }

        return ret;
    }

    /**
     * Counts the subtables
     *
     * @param p_addressTable
     *     the current table
     * @param p_level
     *     the level of the table
     * @param p_count
     *     the table counts
     */
    private void countTables(final long p_addressTable, final int p_level, final int[] p_count) {
        long entry;

        p_count[p_level]++;

        if (p_level == LID_TABLE_LEVELS) {
            for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
                entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;

                if (entry > 0) {
                    countTables(entry, p_level - 1, p_count);
                }
            }
        } else {
            for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
                entry = readEntry(p_addressTable, i) & BITMASK_ADDRESS;

                if (entry > 0) {
                    if (p_level > 1) {
                        countTables(entry, p_level - 1, p_count);
                    } else {
                        p_count[0]++;
                    }
                }
            }
        }
    }

    // Classes

    /**
     * Stores free LocalIDs
     *
     * @author Florian Klein
     *         30.04.2014
     */
    private final class LIDStore {

        // Constants
        private static final int STORE_CAPACITY = 100000;

        // Attributes
        private final long[] m_localIDs;
        private int m_position;
        // available free lid elements stored in our array
        private int m_count;
        // This counts the total available lids in the array
        // as well as elements that are still allocated
        // (because they don't fit into the local array anymore)
        // but not valid -> zombies
        private long m_overallCount;

        // Constructors

        /**
         * Creates an instance of LIDStore
         */
        private LIDStore() {
            m_localIDs = new long[STORE_CAPACITY];
            m_position = 0;
            m_count = 0;

            m_overallCount = 0;
        }

        // Methods

        /**
         * Gets a free LocalID
         *
         * @return a free LocalID
         */
        public long get() {
            long ret = -1;

            if (m_overallCount > 0) {
                if (m_count == 0) {
                    fill();
                }

                if (m_count > 0) {
                    ret = m_localIDs[m_position];

                    m_position = (m_position + 1) % m_localIDs.length;
                    m_count--;
                    m_overallCount--;
                }
            }

            return ret;
        }

        /**
         * Puts a free LocalID
         *
         * @param p_localID
         *     a LocalID
         * @return True if adding an entry to our local ID store was successful, false otherwise.
         */
        public boolean put(final long p_localID) {
            boolean ret;

            if (m_count < m_localIDs.length) {
                m_localIDs[m_position + m_count] = p_localID;

                m_count++;
                ret = true;
            } else {
                ret = false;
            }

            m_overallCount++;

            return ret;
        }

        /**
         * Fills the store
         */
        private void fill() {
            findFreeLIDs();
        }

        /**
         * Finds free LIDs in the CIDTable
         */
        private void findFreeLIDs() {
            findFreeLIDs(readEntry(m_addressTableDirectory, m_nodeIdHook.getNodeId() & NID_LEVEL_BITMASK) & BITMASK_ADDRESS, LID_TABLE_LEVELS - 1, 0);
        }

        /**
         * Finds free LIDs in the CIDTable
         *
         * @param p_addressTable
         *     the table
         * @param p_level
         *     the table level
         * @param p_offset
         *     the offset of the LID
         * @return true if free LIDs were found, false otherwise
         */
        private boolean findFreeLIDs(final long p_addressTable, final int p_level, final long p_offset) {
            boolean ret = false;
            long localID;
            long entry;

            for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
                // Read table entry
                entry = readEntry(p_addressTable, i);

                if (p_level > 0) {
                    if (entry > 0) {
                        // Get free LocalID in the next table
                        if (!findFreeLIDs(entry & BITMASK_ADDRESS, p_level - 1, i << BITS_PER_LID_LEVEL * p_level)) {
                            // Mark the table as full
                            entry |= FULL_FLAG;
                            writeEntry(p_addressTable, i, entry);
                        } else {
                            ret = true;
                        }
                    }
                } else {
                    // check if we got an entry referencing a zombie
                    if ((entry & DELETED_FLAG) > 0 && (entry & BITMASK_ADDRESS) > 0) {
                        localID = p_offset + i;

                        // cleanup zombie in table
                        writeEntry(p_addressTable, i, 0);

                        m_localIDs[m_position + m_count] = localID;
                        m_count++;

                        ret = true;
                    }
                }

                if (m_count == m_localIDs.length || m_count == m_overallCount) {
                    break;
                }
            }

            return ret;
        }
    }

    /**
     * Special interface to allow booting the memory manager before the
     * boot component but still getting the node id after the boot
     * component has booted.
     */
    interface GetNodeIdHook {

        /**
         * Returns the NodeID
         *
         * @return the NodeID
         */
        short getNodeId();
    }

    /**
     * Cache for translated addresses
     */
    private static final class TranslationCache {

        private long[] m_chunkIDs;
        private long[] m_tableLevel0Addr;
        private int m_cachePos;

        /**
         * Constructor
         *
         * @param p_size
         *     Number of entries for the cache
         */
        TranslationCache(final int p_size) {
            m_chunkIDs = new long[p_size];
            m_tableLevel0Addr = new long[p_size];
            m_cachePos = 0;

            for (int i = 0; i < p_size; i++) {
                m_chunkIDs[i] = -1;
                m_tableLevel0Addr[i] = -1;
            }
        }

        /**
         * Try to get the table level 0 entry for the chunk id
         *
         * @param p_chunkID
         *     Chunk id for cache lookup of table level 0
         * @return Address of level 0 table or -1 if not cached
         */
        long getTableLevel0(final long p_chunkID) {
            long tableLevel0IDRange = p_chunkID & 0xFFFFFFFFFFFFF000L;

            for (int i = 0; i < m_chunkIDs.length; i++) {
                if (m_chunkIDs[i] == tableLevel0IDRange) {
                    return m_tableLevel0Addr[i];
                }
            }

            return -1;
        }

        /**
         * Put a new entry into the cache
         *
         * @param p_chunkID
         *     Chunk id of the table level 0 to be cached
         * @param p_addressTable
         *     Address of the level 0 table
         */
        void putTableLevel0(final long p_chunkID, final long p_addressTable) {
            m_chunkIDs[m_cachePos] = p_chunkID & 0xFFFFFFFFFFFFF000L;
            m_tableLevel0Addr[m_cachePos] = p_addressTable;
            m_cachePos = (m_cachePos + 1) % m_chunkIDs.length;
        }
    }
}
