/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.lock.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;

/**
 * Request for unlocking Chunks on a remote node
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public class UnlockMessage extends Message {

    private long m_chunkID = ChunkID.INVALID_ID;
    private byte m_lockCode;

    /**
     * Creates an instance of UnlockRequest as a receiver.
     */
    public UnlockMessage() {
        super();
    }

    /**
     * Creates an instance of UnlockRequest as a sender
     *
     * @param p_destination
     *         the destination node ID.
     * @param p_writeLock
     *         True for the write lock, false for read lock.
     * @param p_chunkID
     *         Chunk id to unlock
     */
    public UnlockMessage(final short p_destination, final boolean p_writeLock, final long p_chunkID) {
        super(p_destination, DXRAMMessageTypes.LOCK_MESSAGES_TYPE, LockMessages.SUBTYPE_UNLOCK_MESSAGE);

        m_chunkID = p_chunkID;

        if (!p_writeLock) {
            m_lockCode = 1;
        } else {
            m_lockCode = 2;
        }
    }

    /**
     * Get the chunk ID to unlock (when receiving).
     *
     * @return Chunk ID to unlock.
     */
    public long getChunkID() {
        return m_chunkID;
    }

    /**
     * Get the lock operation to execute (when receiving).
     *
     * @return True for write lock, false read lock.
     */
    public boolean isWriteLockOperation() {
        return m_lockCode != 1;
    }

    @Override
    protected final int getPayloadLength() {
        return Byte.BYTES + Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_lockCode);
        p_exporter.writeLong(m_chunkID);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lockCode = p_importer.readByte(m_lockCode);
        m_chunkID = p_importer.readLong(m_chunkID);
    }

}
