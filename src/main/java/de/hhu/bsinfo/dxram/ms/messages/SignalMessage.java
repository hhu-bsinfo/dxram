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

package de.hhu.bsinfo.dxram.ms.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.ms.Signal;

/**
 * Message to send signal codes from master to slave or vice versa
 * to abort execution for example
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SignalMessage extends Message {
    private Signal m_signal;

    /**
     * Creates an instance of SignalMessage.
     * This constructor is used when receiving this message.
     */
    public SignalMessage() {
        super();
    }

    /**
     * Creates an instance of SignalMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_signal
     *         signal to send
     */
    public SignalMessage(final short p_destination, final Signal p_signal) {
        super(p_destination, DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE,
                MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE);

        m_signal = p_signal;
    }

    /**
     * Get the signal triggered
     *
     * @return Signal
     */
    public Signal getSignal() {
        return m_signal;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_signal.ordinal());
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_signal = Signal.values()[p_importer.readInt(0)];
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }
}
