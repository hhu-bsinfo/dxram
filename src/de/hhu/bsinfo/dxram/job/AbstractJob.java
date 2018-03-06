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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
    private long[] m_parameterChunkIDs;

    // allow the job system to access the listeners
    ArrayList<JobEventListener> m_eventListeners = new ArrayList<>();

    // nasty, but the only way to get access to services/the API for
    // external/user code
    private DXRAMServiceAccessor m_serviceAccessor;

    private static Map<Short, Class<? extends AbstractJob>> m_registeredJobTypes = new HashMap<Short, Class<? extends AbstractJob>>();

    /**
     * Constructor
     *
     * @param p_parameterChunkIDs
     *         ChunkIDs, which are passed as parameters to the Job on execution.
     *         The max count is 255, which is plenty enough.
     */
    public AbstractJob(final long... p_parameterChunkIDs) {
        assert p_parameterChunkIDs.length <= 255;
        assert p_parameterChunkIDs.length >= 0;
        if (p_parameterChunkIDs != null) {
            m_parameterChunkIDs = p_parameterChunkIDs;
        } else {
            m_parameterChunkIDs = new long[0];
        }
    }

    /**
     * Constructor with no chunkID parameters.
     */
    public AbstractJob() {
        m_parameterChunkIDs = new long[0];
    }

    /**
     * Create an instance of a Job using a previously registered type ID.
     * This is used for serialization/creating Jobs from serialized data.
     *
     * @param p_typeID
     *         Type ID of the job to create.
     * @return Job object.
     * @throws JobRuntimeException
     *         If creating an instance failed or no Job class is registered for the specified type ID.
     */
    public static AbstractJob createInstance(final short p_typeID) {
        AbstractJob job = null;
        Class<? extends AbstractJob> clazz = m_registeredJobTypes.get(p_typeID);

        if (clazz != null) {
            try {
                job = clazz.newInstance();
            } catch (final InstantiationException | IllegalAccessException e) {
                throw new JobRuntimeException("Creating instance for job type ID " + p_typeID + " failed.", e);
            }
        } else {
            throw new JobRuntimeException("Creating instance for job type ID " + p_typeID + " failed, no class registered for type " + p_typeID);
        }

        return job;
    }

    /**
     * Register a Job with its type ID. Make sure to do this for every Job subclass you create.
     *
     * @param p_typeID
     *         Type ID for the Job class.
     * @param p_clazz
     *         The class to register for the specified ID.
     * @throws JobRuntimeException
     *         If another Job class was already registered with the specified type ID.
     */
    public static void registerType(final short p_typeID, final Class<? extends AbstractJob> p_clazz) {
        Class<? extends AbstractJob> clazz = m_registeredJobTypes.putIfAbsent(p_typeID, p_clazz);
        if (clazz != null) {
            throw new JobRuntimeException("Job type with ID " + p_typeID + " already registered for class " + clazz);
        }
    }

    /**
     * Get the type ID of this Job object.
     *
     * @return Type ID.
     */
    public abstract short getTypeID();

    /**
     * Get the ID of this job.
     *
     * @return ID of job (16 bits NodeID/CreatorID + 48 bits local JobID).
     */
    public long getID() {
        return m_id;
    }

    /**
     * Execute this job.
     *
     * @param p_nodeID
     *         NodeID of the node this job is excecuted on.
     */
    public void execute(final short p_nodeID) {
        execute(p_nodeID, m_parameterChunkIDs);
    }

    @Override
    public String toString() {
        return "Job[m_ID " + Long.toHexString(m_id) + "]";
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
        m_parameterChunkIDs = p_importer.readLongArray(m_parameterChunkIDs);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_id);
        p_exporter.writeLongArray(m_parameterChunkIDs);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES + ObjectSizeUtil.sizeofLongArray(m_parameterChunkIDs);
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
     * Implement this function and put your code to be executed with this job here.
     *
     * @param p_nodeID
     *         NodeID this job is executed on.
     * @param p_chunkIDs
     *         Parameters this job was created with.
     */
    protected abstract void execute(final short p_nodeID, long[] p_chunkIDs);

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
}
