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

package de.hhu.bsinfo.dxram.migration.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Sends a Migration Message which requests a remote migration request
 *
 * @author Mike Birkhoff, michael.birkhoff@hhu.de, 15.07.2016
 */
public class MigrationRemoteMessage extends AbstractMessage {

    // Attributes
    private long m_chunkID;
    private short m_target;

    // Constructors

    /**
     * Creates an instance of MigrationRemoteMessage
     */
    public MigrationRemoteMessage() {
        super();
    }

    /**
     * Creates an instance of MigrationRemoteMessage
     *
     * @param p_destination
     *     the destination
     * @param p_cid
     *     the ChunkID
     * @param p_target
     *     the target peer to get the chunk
     */
    public MigrationRemoteMessage(final short p_destination, final long p_cid, final short p_target) {
        super(p_destination, DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_REMOTE_MESSAGE);

        m_chunkID = p_cid;
        m_target = p_target;

    }

    /**
     * Returns the ChunkID
     *
     * @return the ChunkID
     */
    public long getChunkID() {
        return m_chunkID;
    }

    /**
     * get node id to migrate to
     *
     * @return node id to migrate to
     */
    public short getTargetNode() {
        return m_target;
    }

    @Override
    protected final int getPayloadLength() {

        return Long.BYTES + Short.BYTES;
    }

    // Network Data Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {

        p_buffer.putLong(m_chunkID);
        p_buffer.putShort(m_target);

    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {

        m_chunkID = p_buffer.getLong();
        m_target = p_buffer.getShort();

    }
}
