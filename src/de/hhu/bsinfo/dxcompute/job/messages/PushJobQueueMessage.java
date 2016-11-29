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

package de.hhu.bsinfo.dxcompute.job.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.dxcompute.job.AbstractJob;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Push a job to the queue of another node
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class PushJobQueueMessage extends AbstractMessage {
    private AbstractJob m_job;
    private byte m_callbackJobEventBitMask;

    /**
     * Creates an instance of PushJobQueueRequest.
     * This constructor is used when receiving this message.
     */
    public PushJobQueueMessage() {
        super();
    }

    /**
     * Creates an instance of PushJobQueueRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     * @param p_job
     *            The job tu push
     * @param p_callbackJobEventBitMask
     *            Bit mask indicating the events the other node wants to be notified about
     */
    public PushJobQueueMessage(final short p_destination, final AbstractJob p_job, final byte p_callbackJobEventBitMask) {
        super(p_destination, DXComputeMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE);

        m_job = p_job;
        m_callbackJobEventBitMask = p_callbackJobEventBitMask;
    }

    /**
     * Get the job of this request.
     * @return Job.
     */
    public AbstractJob getJob() {
        return m_job;
    }

    /**
     * Get the bitmask to be used when initiating callbacks to the remote
     * side sending this message.
     * @return BitMask for callbacks to remote.
     */
    public byte getCallbackJobEventBitMask() {
        return m_callbackJobEventBitMask;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

        p_buffer.put(m_callbackJobEventBitMask);
        p_buffer.putShort(m_job.getTypeID());
        exporter.exportObject(m_job);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

        m_callbackJobEventBitMask = p_buffer.get();
        m_job = AbstractJob.createInstance(p_buffer.getShort());

        importer.importObject(m_job);
    }

    @Override
    protected final int getPayloadLength() {
        return Byte.BYTES + Short.BYTES + m_job.sizeofObject();
    }
}
