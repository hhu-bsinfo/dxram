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

package de.hhu.bsinfo.dxram.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.job.event.JobEventListener;
import de.hhu.bsinfo.dxram.job.event.JobEvents;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Base class for an job that can be executed by the
 * JobService/JobComponent.
 * Jobs can either be used internally by DXRAM, pushed through
 * the JobComponent, or externally by the user, pushed through
 * the JobService.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public abstract class AbstractJob implements Importable, Exportable {
    private long m_id = JobID.INVALID_ID;

    // allow the job system to access the listeners
    ArrayList<JobEventListener> m_eventListeners = new ArrayList<>();

    // nasty, but the only way to get access to services/the API for
    // external/user code
    private DXRAMServiceAccessor m_serviceAccessor;

    private JobExecutable m_executable = MOCK_EXECUTABLE;

    private byte[] m_executableBytes;

    private boolean m_isExecutableDeserialized = false;

    /**
     * Constructor.
     */
    public AbstractJob() {

    }

    /**
     * Get the ID of this job.
     *
     * @return ID of job (16 bits NodeID/CreatorID + 48 bits local JobID).
     */
    public long getID() {
        return m_id;
    }

    /**
     * Get the type ID of this Job object.
     *
     * @return Type ID.
     */
    public abstract short getTypeID();

    /**
     * Execute this job.
     */
    public abstract void execute();

    @Override
    public String toString() {
        return "Job[m_ID " + Long.toHexString(m_id) + ']';
    }

    /**
     * Register a listener, which receives events specified by the bit mask it
     * returns.
     *
     * @param p_listener
     *         Listener to register to listen to job events.
     */
    public void registerEventListener(final JobEventListener p_listener) {
        m_eventListeners.add(p_listener);
    }

    // -------------------------------------------------------------------

    @Override
    public void importObject(final Importer p_importer) {
        m_id = p_importer.readLong(m_id);
        m_executableBytes = p_importer.readByteArray(m_executableBytes);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_id);
        p_exporter.writeByteArray(m_executableBytes);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES + ObjectSizeUtil.sizeofByteArray(m_executableBytes);
    }

    // -------------------------------------------------------------------

    /**
     * Set the ID for this job. Has to be done before scheduling it.
     *
     * @param p_id
     *         ID to set for this job.
     */
    void setID(final long p_id) {
        m_id = p_id;
    }

    /**
     * Set the service accessor to allow access to all DXRAM services (refer to DXRAMServiceAcessor class for details
     * and important notes).
     *
     * @param p_serviceAccessor
     *         Service accessor to set.
     */
    void setServiceAccessor(final DXRAMServiceAccessor p_serviceAccessor) {
        m_serviceAccessor = p_serviceAccessor;
    }

    /**
     * Called when the job is scheduled for execution by the system.
     *
     * @param p_sourceNodeId
     *         Node if of the source calling.
     */
    void notifyListenersJobScheduledForExecution(final short p_sourceNodeId) {
        for (JobEventListener listener : m_eventListeners) {
            if ((listener.getJobEventBitMask() & JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID) > 0) {
                listener.jobEventTriggered(JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID, m_id, p_sourceNodeId);
            }
        }
    }

    /**
     * Called when the job got assigned to a worker and is in before getting executed.
     *
     * @param p_sourceNodeId
     *         Node if of the source calling.
     */
    void notifyListenersJobStartsExecution(final short p_sourceNodeId) {
        for (JobEventListener listener : m_eventListeners) {
            if ((listener.getJobEventBitMask() & JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID) > 0) {
                listener.jobEventTriggered(JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID, m_id, p_sourceNodeId);
            }
        }
    }

    /**
     * Called when a job has finished execution by a worker.
     *
     * @param p_sourceNodeId
     *         Node if of the source calling.
     */
    void notifyListenersJobFinishedExecution(final short p_sourceNodeId) {
        for (JobEventListener listener : m_eventListeners) {
            if ((listener.getJobEventBitMask() & JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID) > 0) {
                listener.jobEventTriggered(JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID, m_id, p_sourceNodeId);
            }
        }
    }

    // -------------------------------------------------------------------

    /**
     * Get services from DXRAM within this job. This is allowed for external jobs, only.
     * These jobs are pushed to the system from outside/the user via JobService.
     * If a job was pushed internally (via the JobComponent) this returns null
     * as we do not allow access to services internally (breaking isolation concept).
     *
     * @param <T>
     *         Type of the service to get
     * @param p_class
     *         Class of the service to get. If the service has different implementations, use the common interface
     *         or abstract class to get the registered instance.
     * @return Reference to the service if available and enabled, null otherwise.
     */
    protected <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        if (m_serviceAccessor != null) {
            return m_serviceAccessor.getService(p_class);
        } else {
            return null;
        }
    }

    public JobExecutable getExecutable() {
        if (m_isExecutableDeserialized) {
            return m_executable;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(m_executableBytes); ObjectInput in = new ObjectInputStream(bis)) {
            m_executable = (JobExecutable) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }

        m_isExecutableDeserialized = true;

        return m_executable;
    }

    public void setExecutable(final JobExecutable p_executable) {
        m_executable = p_executable;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(m_executable);
            m_executableBytes = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        getExecutable().execute(m_serviceAccessor);
    }

    private static final JobExecutable MOCK_EXECUTABLE = (JobExecutable) p_serviceAccessor -> {};
}
