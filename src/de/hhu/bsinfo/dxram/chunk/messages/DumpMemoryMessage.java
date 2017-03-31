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

package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Message to dump the chunk memory
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.02.2017
 */
public class DumpMemoryMessage extends AbstractMessage {

    private String m_fileName;

    /**
     * Creates an instance of DumpMemoryMessage.
     * This constructor is used when receiving this message.
     */
    public DumpMemoryMessage() {
        super();
    }

    /**
     * Creates an instance of DumpMemoryMessage
     *
     * @param p_destination
     *     the destination
     * @param p_fileName
     *     Output file to dump the memory to
     */
    public DumpMemoryMessage(final short p_destination, final String p_fileName) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE);

        m_fileName = p_fileName;
    }

    /**
     * Output file to dump the memory to
     *
     * @return Output file name
     */
    public String getFileName() {
        return m_fileName;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_fileName.getBytes().length;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        byte[] arr = m_fileName.getBytes();
        p_buffer.putInt(arr.length);
        p_buffer.put(arr);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        byte[] arr = new byte[p_buffer.getInt()];
        p_buffer.get(arr);
        m_fileName = new String(arr);
    }

}
