
package de.hhu.bsinfo.dxcompute.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxcompute.job.event.JobEventListener;
import de.hhu.bsinfo.dxcompute.job.event.JobEvents;
import de.hhu.bsinfo.dxcompute.job.messages.JobEventTriggeredMessage;
import de.hhu.bsinfo.dxcompute.job.messages.JobMessages;
import de.hhu.bsinfo.dxcompute.job.messages.PushJobQueueMessage;
import de.hhu.bsinfo.dxcompute.job.messages.StatusRequest;
import de.hhu.bsinfo.dxcompute.job.messages.StatusResponse;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Service interface to schedule executables jobs. Use this to execute code
 * concurrently and even remotely with DXRAM.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class JobService extends AbstractDXRAMService implements MessageReceiver, JobEventListener {

	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private AbstractJobComponent m_job;
	private StatisticsComponent m_statistics;
	private NetworkComponent m_network;

	private JobStatisticsRecorderIDs m_statisticsRecorderIDs;

	private AtomicLong m_jobIDCounter = new AtomicLong(0);

	private Map<Long, Pair<Byte, AbstractJob>> m_remoteJobCallbackMap = new HashMap<Long, Pair<Byte, AbstractJob>>();

	/**
	 * Register a new implementation/type of Job class.
	 * Make sure to register all your Job classes.
	 * @param p_typeID
	 *            Type ID for the job to register.
	 * @param p_clazz
	 *            Class to register for the specified ID.
	 */
	public void registerJobType(final short p_typeID, final Class<? extends AbstractJob> p_clazz) {
		// #if LOGGER >= DEBUG
		// // m_logger.debug(getClass(), "Registering job type " + p_typeID + " for class " + p_clazz);
		// #endif /* LOGGER >= DEBUG */

		AbstractJob.registerType(p_typeID, p_clazz);
	}

	/**
	 * Schedule a job for execution (local).
	 * @param p_job
	 *            Job to be scheduled for execution.
	 * @return True if scheduling was successful, false otherwise.
	 */
	public long pushJob(final AbstractJob p_job) {
		// early return
		if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "A superpeer is not allowed to submit jobs.");
			// #endif /* LOGGER >= ERROR */
			return JobID.INVALID_ID;
		}

		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_submit);
		// #endif /* STATISTICS */

		long jobId = JobID.createJobID(m_boot.getNodeID(), m_jobIDCounter.incrementAndGet());

		// nasty way to access the services...feel free to have a better solution for this
		p_job.setServiceAccessor(getServiceAccessor());
		p_job.setID(jobId);
		if (!m_job.pushJob(p_job)) {
			jobId = JobID.INVALID_ID;
			p_job.setID(jobId);
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_submit);
		// #endif /* STATISTICS */

		return jobId;
	}

	/**
	 * Schedule a job for remote execution. The job is sent to the node specified and
	 * scheduled for execution there.
	 * @param p_job
	 *            Job to schedule.
	 * @param p_nodeID
	 *            ID of the node to schedule the job on.
	 * @return Valid job ID assigned to the submitted job, false otherwise.
	 */
	public long pushJobRemote(final AbstractJob p_job, final short p_nodeID) {
		// early return
		if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "A superpeer is not allowed to submit remote jobs.");
			// #endif /* LOGGER >= ERROR */
			return JobID.INVALID_ID;
		}

		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remoteSubmit);
		// #endif /* STATISTICS */

		long jobId = JobID.createJobID(m_boot.getNodeID(), m_jobIDCounter.incrementAndGet());
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

			m_remoteJobCallbackMap.put(jobId, new Pair<Byte, AbstractJob>(mergedCallbackBitMask, p_job));
		}

		PushJobQueueMessage message = new PushJobQueueMessage(p_nodeID, p_job, mergedCallbackBitMask);
		NetworkErrorCodes error = m_network.sendMessage(message);
		if (error != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Sending push job queue message to node " + p_nodeID + " failed: " + error);
			// #endif /* LOGGER >= ERROR */
			jobId = JobID.INVALID_ID;
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remoteSubmit);
		// #endif /* STATISTICS */

		// set jobid again to mark possible failure
		p_job.setID(jobId);
		return jobId;
	}

	/**
	 * Wait for all locally scheduled and currently executing jobs to finish.
	 * @return True if waiting was successful and all jobs finished, false otherwise.
	 */
	public boolean waitForLocalJobsToFinish() {
		return m_job.waitForSubmittedJobsToFinish();
	}

	/**
	 * Wait for all jobs including remote ones to finish
	 * @return True if all jobs finished successfully, false otherwise.
	 */
	public boolean waitForAllJobsToFinish() {
		int successCount = 0;
		// if we checked all job systems successfully
		// 5 times in a row, we consider this as a full termination
		while (successCount < 5) {
			// wait for local jobs to finish first
			if (!m_job.waitForSubmittedJobsToFinish()) {
				return false;
			}

			// now check for remote peers
			List<Short> peers = m_boot.getIDsOfOnlinePeers();
			for (short peer : peers) {
				StatusRequest request = new StatusRequest(peer);
				NetworkErrorCodes error = m_network.sendSync(request);
				if (error != NetworkErrorCodes.SUCCESS) {
					// #if LOGGER >= ERROR
					m_logger.error(getClass(),
							"Sending get status request to wait for termination to " + peer + " failed: " + error);
					// #endif /* LOGGER >= ERROR */
					// abort here as well, as we do not know what happened exactly
					return false;
				} else {
					StatusResponse response = request.getResponse(StatusResponse.class);
					if (response.getStatus().getNumberOfUnfinishedJobs() != 0) {
						successCount = 0;

						// not done, yet...sleep a little and try again
						try {
							Thread.sleep(1000);
						} catch (final InterruptedException e) {}
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

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		// #if LOGGER == TRACE
		// // m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);
		// #endif /* LOGGER == TRACE */
		if (p_message != null) {
			if (p_message.getType() == JobMessages.TYPE) {
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
		// #if LOGGER == TRACE
		// // m_logger.trace(getClass(), "Exiting incomingMessage");
		// #endif /* LOGGER == TRACE */
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public byte getJobEventBitMask() {
		// we need this to redirect callbacks for jobs, which got
		// submitted by a remote instance to us
		return JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID
				| JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID
				| JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID;
	}

	@Override
	public void jobEventTriggered(final byte p_eventId, final long p_jobId, final short p_sourceNodeId) {
		Pair<Byte, AbstractJob> job = m_remoteJobCallbackMap.get(p_jobId);
		if (job != null) {
			// check if any remote source is interested in this
			if ((job.m_first & p_eventId) > 0) {
				// we have to redirect this to the remote source
				// clear the event we are triggering
				job.m_first = (byte) (job.m_first & (~p_eventId));
				if (job.m_first == 0) {
					// no further events to trigger for remote, remove from map
					m_remoteJobCallbackMap.remove(p_jobId);
				}

				JobEventTriggeredMessage message =
						new JobEventTriggeredMessage(JobID.getCreatorID(p_jobId), p_jobId, p_eventId);
				NetworkErrorCodes error = m_network.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					// #if LOGGER >= ERROR
					m_logger.error(getClass(),
							"Triggering job event '" + p_eventId + "' for job " + job.m_second + "' failed: " + error);
					// #endif /* LOGGER >= ERROR */
				}
			}
		} else {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Getting stored callbacks from map for callback to job id '"
							+ Long.toHexString(p_jobId) + "' failed.");
			// #endif /* LOGGER >= ERROR */
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_boot = getComponent(AbstractBootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_job = getComponent(AbstractJobComponent.class);
		// #ifdef STATISTICS
		m_statistics = getComponent(StatisticsComponent.class);
		// #endif /* STATISTICS */
		m_network = getComponent(NetworkComponent.class);

		registerNetworkMessages();
		registerNetworkMessageListener();

		// #ifdef STATISTICS
		registerStatisticsOperations();
		// #endif /* STATISTICS */

		AbstractJob.registerType(JobNull.MS_TYPE_ID, JobNull.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_boot = null;
		m_logger = null;
		m_job = null;
		m_statistics = null;
		m_network = null;

		return true;
	}

	@Override
	protected boolean isServiceAccessor() {
		return true;
	}

	// ------------------------------------------------------------------------------------------

	/**
	 * Register network messages used here.
	 */
	private void registerNetworkMessages() {
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE,
				PushJobQueueMessage.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_STATUS_REQUEST, StatusRequest.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_STATUS_RESPONSE, StatusResponse.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE,
				JobEventTriggeredMessage.class);
	}

	/**
	 * Register listener for incoming messages.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(PushJobQueueMessage.class, this);
		m_network.register(StatusRequest.class, this);
		m_network.register(JobEventTriggeredMessage.class, this);
	}

	/**
	 * Register statistics stuff.
	 */
	private void registerStatisticsOperations() {
		m_statisticsRecorderIDs = new JobStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(this.getClass());

		m_statisticsRecorderIDs.m_operations.m_submit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				JobStatisticsRecorderIDs.Operations.MS_SUBMIT);
		m_statisticsRecorderIDs.m_operations.m_remoteSubmit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				JobStatisticsRecorderIDs.Operations.MS_REMOTE_SUBMIT);
		m_statisticsRecorderIDs.m_operations.m_incomingSubmit = m_statistics
				.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_INCOMING_SUBMIT);
	}

	/**
	 * Handle incoming push queue request.
	 * @param p_request
	 *            Incoming request.
	 */
	private void incomingPushJobQueueMessage(final PushJobQueueMessage p_request) {
		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingSubmit);
		// #endif /* STATISTICS */

		AbstractJob job = p_request.getJob();
		job.setServiceAccessor(getServiceAccessor());

		// register ourselves as listener to event callbacks
		// and redirect them to the remote source
		job.registerEventListener(this);
		m_remoteJobCallbackMap.put(job.getID(),
				new Pair<Byte, AbstractJob>(p_request.getCallbackJobEventBitMask(), job));

		if (!m_job.pushJob(job)) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Scheduling job " + job + " failed.");
			// #endif /* LOGGER >= ERROR */
			m_remoteJobCallbackMap.remove(job.getID());
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingSubmit);
		// #endif /* STATISTICS */
	}

	/**
	 * Handle incoming status request.
	 * @param p_request
	 *            Incoming request.
	 */
	private void incomingStatusRequest(final StatusRequest p_request) {
		Status status = new Status();
		status.m_numUnfinishedJobs = m_job.getNumberOfUnfinishedJobs();

		StatusResponse response = new StatusResponse(p_request, status);
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Sending StatusResponse for " + p_request + " failed: " + error);
			// #endif /* LOGGER >= ERROR */
		}
	}

	/**
	 * Dispatch for JobEventTriggeredMessage
	 * @param p_message
	 *            The incoming message
	 */
	private void incomingJobEventTriggeredMessage(final JobEventTriggeredMessage p_message) {
		Pair<Byte, AbstractJob> job = m_remoteJobCallbackMap.get(p_message.getJobID());
		if (job != null) {
			// check if we really registered for what we got from the remote instance
			if ((job.m_first & p_message.getEventId()) > 0) {
				// redirect the remote event triggering to our locally
				// registered listeners

				// clear the event we are triggering
				job.m_first = (byte) (job.m_first & (~p_message.getEventId()));
				if (job.m_first == 0) {
					// no further events to trigger for remote, remove from map
					m_remoteJobCallbackMap.remove(p_message.getJobID());
				}

				// TODO use event system here to avoid blocking the network thread for too long?
				switch (p_message.getEventId()) {
				case JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID:
					job.m_second.notifyListenersJobScheduledForExecution(p_message.getSource());
					break;
				case JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID:
					job.m_second.notifyListenersJobStartsExecution(p_message.getSource());
					break;
				case JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID:
					job.m_second.notifyListenersJobFinishedExecution(p_message.getSource());
					break;
				default:
					assert 1 == 2;
					break;
				}
			} else {
				// should not happen, because we registered for specific events, only
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"Getting remote callback for unregistered event '" + p_message.getEventId() + "' on job id '"
								+ Long.toHexString(p_message.getJobID()) + "'.");
				// #endif /* LOGGER >= ERROR */
			}
		} else {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Getting stored callbacks from map for callback to job id '"
							+ Long.toHexString(p_message.getJobID()) + "' failed.");
			// #endif /* LOGGER >= ERROR */
		}
	}

	/**
	 * Status object holding information about the job service.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
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
		public boolean hasDynamicObjectSize() {
			return false;
		}

		@Override
		public int exportObject(final Exporter p_exporter, final int p_size) {
			p_exporter.writeLong(m_numUnfinishedJobs);
			return sizeofObject();
		}

		@Override
		public int importObject(final Importer p_importer, final int p_size) {
			m_numUnfinishedJobs = p_importer.readLong();
			return sizeofObject();
		}
	}
}
