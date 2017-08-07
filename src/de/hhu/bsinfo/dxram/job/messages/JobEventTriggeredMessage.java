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

package de.hhu.bsinfo.dxram.job.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.job.JobID;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;

/**
 * Message indicating a job event was triggered on another node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobEventTriggeredMessage extends Message {
    private long m_jobId = JobID.INVALID_ID;
    private byte m_eventID;

    /**
     * Creates an instance of PushJobQueueRequest.
     * This constructor is used when receiving this message.
     */
    public JobEventTriggeredMessage() {
        super();
    }

    /**
     * Creates an instance of PushJobQueueRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_jobId
     *         Id of the job
     * @param p_eventId
     *         Event id
     */
    public JobEventTriggeredMessage(final short p_destination, final long p_jobId, final byte p_eventId) {
        super(p_destination, DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE);

        m_jobId = p_jobId;
        m_eventID = p_eventId;
    }

    /**
     * Get the job id.
     *
     * @return Job id.
     */
    public long getJobID() {
        return m_jobId;
    }

    /**
     * Get the id of the event triggered.
     *
     * @return Event id.
     */
    public byte getEventId() {
        return m_eventID;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_jobId);
        p_exporter.writeByte(m_eventID);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_jobId = p_importer.readLong(m_jobId);
        m_eventID = p_importer.readByte(m_eventID);
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES + Byte.BYTES;
    }
}
