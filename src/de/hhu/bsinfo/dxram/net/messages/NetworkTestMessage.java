/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.net.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Network message for running tests/benchmarks.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 25.01.2017
 */
public class NetworkTestMessage extends Message {

    private byte[] m_data;

    /**
     * Creates an instance of NetworkTestMessage.
     * This constructor is used when receiving this message.
     */
    public NetworkTestMessage() {
        super();
    }

    /**
     * Creates an instance of NetworkTestMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_messageSize
     *         Size of byte array.
     */
    public NetworkTestMessage(final short p_destination, final int p_messageSize) {
        super(p_destination, DXRAMMessageTypes.NETWORK_MESSAGES_TYPE, NetworkMessages.SUBTYPE_TEST_MESSAGE);

        m_data = new byte[p_messageSize];
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_data);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_data = p_importer.readByteArray(m_data);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_data);
    }
}
