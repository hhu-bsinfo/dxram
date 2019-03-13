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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.job.event.JobEventListener;
import de.hhu.bsinfo.dxram.job.event.JobEvents;
import de.hhu.bsinfo.dxram.job.messages.JobEventTriggeredMessage;
import de.hhu.bsinfo.dxram.job.messages.JobMessages;
import de.hhu.bsinfo.dxram.job.messages.PushJobQueueMessage;
import de.hhu.bsinfo.dxram.job.messages.StatusRequest;
import de.hhu.bsinfo.dxram.job.messages.StatusResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;

/**
 * Service interface to schedule executables jobs. Use this to execute code
 * concurrently and even remotely with DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class JobService extends Service<ModuleConfig> implements MessageReceiver, JobEventListener {
    private static final TimePool SOP_CREATE = new TimePool(JobService.class, "Submit");
    private static final TimePool SOP_REMOTE_SUBMIT = new TimePool(JobService.class, "RemoteSubmit");
    private static final TimePool SOP_INCOMING_SUBMIT = new TimePool(JobService.class, "IncomingSubmit");

    static {
        StatisticsManager.get().registerOperation(JobService.class, SOP_CREATE);
        StatisticsManager.get().registerOperation(JobService.class, SOP_REMOTE_SUBMIT);
        StatisticsManager.get().registerOperation(JobService.class, SOP_INCOMING_SUBMIT);
    }

    // depdendent components
    private BootComponent m_boot;
    private JobComponent m_job;
    private NetworkComponent m_network;
    private PluginComponent m_plugin;

    private final AtomicLong m_jobIDCounter = new AtomicLong(0);

    private final Map<Long, JobEventEntry> m_remoteJobCallbackMap = new HashMap<>();

    /**
     * Create an instance of a task denoted by its task (command) name
     *
     * @param p_jobName
     *         Name of the job class
     * @param p_args
     *         Arguments to provide to the task object
     * @return A task instance
     */
    public Job createJobInstance(final String p_jobName, final Object... p_args) {
        Class<?> clazz;

        try {
            clazz = m_plugin.getClassByName(p_jobName);
        } catch (final ClassNotFoundException ignored) {
            LOGGER.error("Cannot find job class: %s", p_jobName);
            return null;
        }

        if (!clazz.getSuperclass().equals(Job.class)) {
            LOGGER.error("Class '%s' does not extend the Job class");
            return null;
        }

        for (Constructor constructor : clazz.getConstructors()) {

            try {
                return (Job) constructor.newInstance(p_args);
            } catch (final SecurityException | InstantiationException | IllegalAccessException |
                    IllegalArgumentException | InvocationTargetException e) {
            }
        }

        LOGGER.error("Cannot create instance of Job '%s'", p_jobName);
        return null;
    }

    /**
     * Schedule a job for execution (local).
     *
     * @param p_job
     *         Job to be scheduled for execution.
     * @return True if scheduling was successful, false otherwise.
     */
    public long pushJob(final Job p_job) {
        SOP_CREATE.start();

        long jobId = JobID.createJobID(m_boot.getNodeId(), m_jobIDCounter.incrementAndGet());

        // nasty way to access the services...feel free to have a better solution for this
        p_job.setServiceAccessor(getParentEngine());
        p_job.setID(jobId);

        if (!m_job.pushJob(p_job)) {
            jobId = JobID.INVALID_ID;
            p_job.setID(jobId);
        }

        SOP_CREATE.stop();

        return jobId;
    }

    /**
     * Schedule a job for remote execution. The job is sent to the node specified and
     * scheduled for execution there.
     *
     * @param p_job
     *         Job to schedule.
     * @param p_nodeID
     *         ID of the node to schedule the job on.
     * @return Valid job ID assigned to the submitted job, false otherwise.
     */
    public long pushJobRemote(final Job p_job, final short p_nodeID) {
        SOP_REMOTE_SUBMIT.start();

        long jobId = JobID.createJobID(m_boot.getNodeId(), m_jobIDCounter.incrementAndGet());
        p_job.setID(jobId);

        byte mergedCallbackBitMask = 0;
        // check if we got any listeners assigned
        // -> have to register for remote callbacks
        // otherwise don't even bother with it to save time
        if (!p_job.m_eventListeners.isEmpty()) {
            // register for remote callbacks by or'ing all
            // bit vectors of registered listeners
            // this is used to send each event only once
            // over the network and then scattering it to
            // all registered listeners locally
            for (JobEventListener listener : p_job.m_eventListeners) {
                mergedCallbackBitMask |= listener.getJobEventBitMask();
            }

            m_remoteJobCallbackMap.put(jobId, new JobEventEntry(mergedCallbackBitMask, p_job));
        }

        PushJobQueueMessage message = new PushJobQueueMessage(p_nodeID, p_job, mergedCallbackBitMask);
        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {

            LOGGER.error("Sending push job queue message to node 0x%X failed: %s", p_nodeID, e);

            jobId = JobID.INVALID_ID;
        }

        SOP_REMOTE_SUBMIT.stop();

        // set jobid again to mark possible failure
        p_job.setID(jobId);
        return jobId;
    }

    /**
     * Wait for all locally scheduled and currently executing jobs to finish.
     *
     * @return True if waiting was successful and all jobs finished, false otherwise.
     */
    public boolean waitForLocalJobsToFinish() {
        return waitForAllJobsToFinish(true, false);
    }

    /**
     * Wait for all remotely scheduled and currently executing jobs to finish.
     *
     * @return True if waiting was successful and all jobs finished, false otherwise.
     */
    public boolean waitForRemoteJobsToFinish() {
        return waitForAllJobsToFinish(false, true);
    }

    /**
     * Wait for all jobs including remote ones to finish
     *
     * @return True if all jobs finished successfully, false otherwise.
     */
    public boolean waitForAllJobsToFinish() {
        return waitForAllJobsToFinish(true, true);
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.JOB_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE:
                        incomingPushJobQueueMessage((PushJobQueueMessage) p_message);
                        break;
                    case JobMessages.SUBTYPE_STATUS_REQUEST:
                        incomingStatusRequest((StatusRequest) p_message);
                        break;
                    case JobMessages.SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE:
                        incomingJobEventTriggeredMessage((JobEventTriggeredMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        LOGGER.trace("Exiting incomingMessage");
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public byte getJobEventBitMask() {
        // we need this to redirect callbacks for jobs, which got
        // submitted by a remote instance to us
        return JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID | JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID |
                JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID;
    }

    @Override
    public void jobEventTriggered(final byte p_eventId, final long p_jobId, final short p_sourceNodeId) {
        JobEventEntry job = m_remoteJobCallbackMap.get(p_jobId);
        if (job != null) {
            // check if any remote source is interested in this
            if ((job.getEventId() & p_eventId) > 0) {
                // we have to redirect this to the remote source
                // clear the event we are triggering
                job = new JobEventEntry((byte) (job.getEventId() & ~p_eventId), job.getJob());
                if (job.getEventId() == 0) {
                    // no further events to trigger for remote, remove from map
                    m_remoteJobCallbackMap.remove(p_jobId);
                }

                JobEventTriggeredMessage message = new JobEventTriggeredMessage(JobID.getCreatorID(p_jobId), p_jobId,
                        p_eventId);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {

                    LOGGER.error("Triggering job event '%s' for job '%s' failed: %s", p_eventId, job.getJob(), e);

                }
            }
        } else {

            LOGGER.error("Getting stored callbacks from map for callback to job id '0x%X' failed", p_jobId);

        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(BootComponent.class);
        m_job = p_componentAccessor.getComponent(JobComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_plugin = p_componentAccessor.getComponent(PluginComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Register network messages used here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE,
                PushJobQueueMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_STATUS_REQUEST,
                StatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_STATUS_RESPONSE,
                StatusResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.JOB_MESSAGES_TYPE,
                JobMessages.SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE, JobEventTriggeredMessage.class);
    }

    /**
     * Register listener for incoming messages.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_STATUS_REQUEST, this);
        m_network.register(DXRAMMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE, this);
    }

    /**
     * Wait for multiple jobs to finish (local and/or remote)
     *
     * @param p_local
     *         Wait for jobs running locally.
     * @param p_remote
     *         Wait for jobs running remotely.
     * @return True if all jobs finished successfully, false otherwise.
     */
    private boolean waitForAllJobsToFinish(final boolean p_local, final boolean p_remote) {
        assert !(!p_local && !p_remote);

        int successCount = 0;

        // if we checked all job systems successfully
        // 5 times in a row, we consider this as a full termination
        while (successCount < 5) {
            if (p_local) {
                // wait for local jobs to finish first
                if (!m_job.waitForSubmittedJobsToFinish()) {
                    return false;
                }
            }

            if (p_remote) {
                // now check for remote peers
                List<Short> peers = m_boot.getOnlinePeerIds();

                for (int i = 0; i < peers.size(); i++) {
                    // filter own node id
                    if (peers.get(i) == m_boot.getNodeId()) {
                        continue;
                    }

                    StatusRequest request = new StatusRequest(peers.get(i));

                    try {
                        m_network.sendSync(request);
                    } catch (final NetworkException e) {
                        LOGGER.error("Sending get status request to wait for termination to 0x%X failed: %s",
                                peers.get(i), e);

                        // abort here as well, as we do not know what happened exactly
                        return false;
                    }

                    StatusResponse response = request.getResponse(StatusResponse.class);

                    if (response.getStatus().getNumberOfUnfinishedJobs() != 0) {
                        successCount = 0;

                        // not done, yet...sleep a little and try again
                        try {
                            Thread.sleep(1000);
                        } catch (final InterruptedException ignored) {
                        }

                        continue;
                    }
                }
            }

            // we checked all job systems and were successful, but
            // this does not guarantee we are really done due to
            // race conditions...
            // make sure to check a few more times
            successCount++;
        }

        return true;
    }

    /**
     * Handle incoming push queue request.
     *
     * @param p_request
     *         Incoming request.
     */
    private void incomingPushJobQueueMessage(final PushJobQueueMessage p_request) {
        SOP_INCOMING_SUBMIT.start();

        Job job = createJobInstance(p_request.getJobName());
        ByteBuffer buffer = ByteBuffer.wrap(p_request.getJobBlob());
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteBufferImExporter importer = new ByteBufferImExporter(buffer);

        // de-serialize job data
        importer.importObject(job);

        job.setServiceAccessor(getParentEngine());

        // register ourselves as listener to event callbacks
        // and redirect them to the remote source
        job.registerEventListener(this);
        m_remoteJobCallbackMap.put(job.getID(), new JobEventEntry(p_request.getCallbackJobEventBitMask(), job));

        if (!m_job.pushJob(job)) {
            LOGGER.error("Scheduling job %s failed", job);

            m_remoteJobCallbackMap.remove(job.getID());
        }

        SOP_INCOMING_SUBMIT.stop();
    }

    /**
     * Handle incoming status request.
     *
     * @param p_request
     *         Incoming request.
     */
    private void incomingStatusRequest(final StatusRequest p_request) {
        Status status = new Status();
        status.m_numUnfinishedJobs = m_job.getNumberOfUnfinishedJobs();

        StatusResponse response = new StatusResponse(p_request, status);

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending StatusResponse for %s failed: %s", p_request, e);

        }
    }

    /**
     * Dispatch for JobEventTriggeredMessage
     *
     * @param p_message
     *         The incoming message
     */
    private void incomingJobEventTriggeredMessage(final JobEventTriggeredMessage p_message) {
        JobEventEntry job = m_remoteJobCallbackMap.get(p_message.getJobID());
        if (job != null) {
            // check if we really registered for what we got from the remote instance
            if ((job.getEventId() & p_message.getEventId()) > 0) {
                // redirect the remote event triggering to our locally
                // registered listeners

                // clear the event we are triggering
                job = new JobEventEntry((byte) (job.getEventId() & ~p_message.getEventId()), job.getJob());
                if (job.getEventId() == 0) {
                    // no further events to trigger for remote, remove from map
                    m_remoteJobCallbackMap.remove(p_message.getJobID());
                }

                // TODO use event system here to avoid blocking the network thread for too long?
                switch (p_message.getEventId()) {
                    case JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID:
                        job.getJob().notifyListenersJobScheduledForExecution(p_message.getSource());
                        break;
                    case JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID:
                        job.getJob().notifyListenersJobStartsExecution(p_message.getSource());
                        break;
                    case JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID:
                        job.getJob().notifyListenersJobFinishedExecution(p_message.getSource());
                        break;
                    default:
                        assert false;
                        break;
                }
            } else {
                // should not happen, because we registered for specific events, only
                LOGGER.error("Getting remote callback for unregistered event '%d' on job id '0x%X'",
                        p_message.getEventId(), p_message.getJobID());

            }
        } else {
            LOGGER.error("Getting stored callbacks from map for callback to job id '0x%X' failed",
                    p_message.getJobID());

        }
    }

    /**
     * Status object holding information about the job service.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
     */
    public static class Status implements Importable, Exportable {
        private long m_numUnfinishedJobs = -1;

        /**
         * Constructor
         */
        public Status() {

        }

        /**
         * Get the number of unfinished jobs
         *
         * @return Number of unfinished jobs
         */
        public long getNumberOfUnfinishedJobs() {
            return m_numUnfinishedJobs;
        }

        @Override
        public int sizeofObject() {
            return Long.BYTES;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeLong(m_numUnfinishedJobs);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_numUnfinishedJobs = p_importer.readLong(m_numUnfinishedJobs);
        }
    }
}
