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

package de.hhu.bsinfo.dxram.failure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Failure Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.10.2016
 */
public class FailureRequest extends Request {

    // Attributes
    private short m_failedNode;

    // Constructors

    /**
     * Creates an instance of FailureRequest
     */
    public FailureRequest() {
        super();

        m_failedNode = NodeID.INVALID_ID;
    }

    /**
     * Creates an instance of FailureRequest
     *
     * @param p_destination
     *         the destination
     * @param p_failedNode
     *         the NodeID of the failed node
     */
    public FailureRequest(final short p_destination, final short p_failedNode) {
        super(p_destination, DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_REQUEST);

        m_failedNode = p_failedNode;
    }

    // Getters

    /**
     * Get the failed node
     *
     * @return the NodeID
     */
    public final short getFailedNode() {
        return m_failedNode;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_failedNode);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_failedNode = p_importer.readShort(m_failedNode);
    }
}
