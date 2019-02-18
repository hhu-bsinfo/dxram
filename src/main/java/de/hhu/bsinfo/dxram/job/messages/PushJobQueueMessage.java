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

package de.hhu.bsinfo.dxram.job.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Push a job to the queue of another node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class PushJobQueueMessage extends Message {
    private AbstractJob m_job;
    private byte m_callbackJobEventBitMask;

    // for receiving job data
    private short m_jobType;
    private int m_jobBlobSize;
    private byte[] m_jobBlob;

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
     *
     * @param p_destination
     *         the destination node id.
     * @param p_job
     *         The job tu push
     * @param p_callbackJobEventBitMask
     *         Bit mask indicating the events the other node wants to be notified about
     */
    public PushJobQueueMessage(final short p_destination, final AbstractJob p_job,
            final byte p_callbackJobEventBitMask) {
        super(p_destination, DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE);

        m_job = p_job;
        m_jobType = p_job.getTypeID();
        m_jobBlobSize = p_job.sizeofObject();
        m_callbackJobEventBitMask = p_callbackJobEventBitMask;
    }

    /**
     * Get the bitmask to be used when initiating callbacks to the remote
     * side sending this message.
     *
     * @return BitMask for callbacks to remote.
     */
    public byte getCallbackJobEventBitMask() {
        return m_callbackJobEventBitMask;
    }

    /**
     * Get the job type
     *
     * @return Job type id
     */
    public short getJobType() {
        return m_jobType;
    }

    /**
     * Get the job blob/data
     *
     * @return Job data as byte array
     */
    public byte[] getJobBlob() {
        return m_jobBlob;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeCompactNumber(m_job.sizeofObject());
        p_exporter.exportObject(m_job);
        p_exporter.writeByte(m_callbackJobEventBitMask);
        p_exporter.writeShort(m_jobType);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_jobBlobSize = p_importer.readCompactNumber(m_jobBlobSize);
        m_jobBlob = new byte[m_jobBlobSize];
        p_importer.readBytes(m_jobBlob);
        m_callbackJobEventBitMask = p_importer.readByte(m_callbackJobEventBitMask);
        m_jobType = p_importer.readShort(m_jobType);
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        if (m_job != null) {
            int jobSize = m_job.sizeofObject();

            size += ObjectSizeUtil.sizeofCompactedNumber(jobSize) + jobSize;
        } else {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_jobBlobSize) + m_jobBlobSize;
        }

        return size + Byte.BYTES + Short.BYTES;
    }
}
