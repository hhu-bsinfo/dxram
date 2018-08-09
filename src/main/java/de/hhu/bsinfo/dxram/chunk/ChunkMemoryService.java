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

package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;

/**
 * This service extends the normal ChunkService with "direct" memory access methods.
 * Instead of get-ing and put-ing whole chunks, there are situations this is not necessary
 * and is harming performance. Thus, we allow reading/writing primitive data types to
 * offsets within chunks. However, this is limited to local access, only. No automatic
 * redirection of read/write requests to remote nodes because this is meant to optimize
 * algorithms that are already aware of their data locality.
 * We also don't have batch methods i.e. combining multiple read/write requests to
 * multiple chunks. Use it wisely if accessing many chunks this way.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.06.2016
 */
public class ChunkMemoryService extends AbstractDXRAMService<ChunkMemoryServiceConfig> {
    // component dependencies
    private AbstractBootComponent m_boot;
    private MemoryManagerComponent m_memoryManager;

    /**
     * Constructor
     */
    public ChunkMemoryService() {
        super("chunkmem", ChunkMemoryServiceConfig.class);
    }

    /**
     * Read a single byte from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public byte readByte(final long p_chunkID, final int p_offset) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot read data from non local chunk %s", ChunkID.toHexString(p_chunkID));
            return -1;
        }

        m_memoryManager.lockAccess();
        byte val = m_memoryManager.readByte(p_chunkID, p_offset);
        m_memoryManager.unlockAccess();
        return val;
    }

    /**
     * Read a single short from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public short readShort(final long p_chunkID, final int p_offset) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot read data from non local chunk %s", ChunkID.toHexString(p_chunkID));
            return -1;
        }

        m_memoryManager.lockAccess();
        short val = m_memoryManager.readShort(p_chunkID, p_offset);
        m_memoryManager.unlockAccess();
        return val;
    }

    /**
     * Read a single int from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public int readInt(final long p_chunkID, final int p_offset) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot read data from non local chunk %s", ChunkID.toHexString(p_chunkID));
            return -1;
        }

        m_memoryManager.lockAccess();
        int val = m_memoryManager.readInt(p_chunkID, p_offset);
        m_memoryManager.unlockAccess();
        return val;
    }

    /**
     * Read a single long from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public long readLong(final long p_chunkID, final int p_offset) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot read data from non local chunk %s", ChunkID.toHexString(p_chunkID));
            return -1;
        }

        m_memoryManager.lockAccess();
        long val = m_memoryManager.readLong(p_chunkID, p_offset);
        m_memoryManager.unlockAccess();
        return val;
    }

    /**
     * Write a single byte to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeByte(final long p_chunkID, final int p_offset, final byte p_value) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot write data to non local chunk %s", ChunkID.toHexString(p_chunkID));
            return false;
        }

        m_memoryManager.lockAccess();
        boolean ret = m_memoryManager.writeByte(p_chunkID, p_offset, p_value);
        m_memoryManager.unlockAccess();
        return ret;
    }

    /**
     * Write a single short to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeShort(final long p_chunkID, final int p_offset, final short p_value) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot write data to non local chunk %s", ChunkID.toHexString(p_chunkID));
            return false;
        }

        m_memoryManager.lockAccess();
        boolean ret = m_memoryManager.writeShort(p_chunkID, p_offset, p_value);
        m_memoryManager.unlockAccess();
        return ret;
    }

    /**
     * Write a single int to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeInt(final long p_chunkID, final int p_offset, final int p_value) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot write data to non local chunk ", ChunkID.toHexString(p_chunkID));
            return false;
        }

        m_memoryManager.lockAccess();
        boolean ret = m_memoryManager.writeInt(p_chunkID, p_offset, p_value);
        m_memoryManager.unlockAccess();
        return ret;
    }

    /**
     * Write a single long to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeLong(final long p_chunkID, final int p_offset, final long p_value) {
        if (ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeId()) {
            LOGGER.error("Cannot write data to non local chunk ", ChunkID.toHexString(p_chunkID));
            return false;
        }

        m_memoryManager.lockAccess();
        boolean ret = m_memoryManager.writeLong(p_chunkID, p_offset, p_value);
        m_memoryManager.unlockAccess();
        return ret;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
