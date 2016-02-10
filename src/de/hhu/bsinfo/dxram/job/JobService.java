package de.hhu.bsinfo.dxram.job;

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.job.messages.JobMessages;
import de.hhu.bsinfo.dxram.job.messages.PushJobQueueRequest;
import de.hhu.bsinfo.dxram.job.messages.PushJobQueueResponse;
import de.hhu.bsinfo.dxram.job.messages.StatusRequest;
import de.hhu.bsinfo.dxram.job.messages.StatusResponse;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Service interface to schedule executables jobs. Use this to execute code
 * concurrently and even remotely with DXRAM.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class JobService extends DXRAMService implements MessageReceiver {

	private BootComponent m_boot = null;
	private LoggerComponent m_logger = null;
	private JobComponent m_job = null;
	private StatisticsComponent m_statistics = null;
	private NetworkComponent m_network = null;
	
	private JobStatisticsRecorderIDs m_statisticsRecorderIDs = null;
	
	/**
	 * Register a new implementation/type of Job class.
	 * Make sure to register all your Job classes.
	 * @param p_typeID Type ID for the job to register.
	 * @param p_clazz Class to register for the specified ID.
	 */
	public void registerJobType(final short p_typeID, final Class<? extends Job> p_clazz)
	{
		m_logger.debug(getClass(), "Registering job type " + p_typeID + " for class " + p_clazz);
		Job.registerType(p_typeID, p_clazz);
	}
	
	/**
	 * Schedule a job for execution (local).
	 * @param p_job Job to be scheduled for execution.
	 * @return True if scheduling was successful, false otherwise.
	 */
	public boolean pushJob(final Job p_job)
	{
		// early return
		if (m_boot.getNodeRole() == NodeRole.SUPERPEER)
		{
			m_logger.error(getClass(), "A superpeer is not allowed to submit jobs.");
			return false;
		}
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_submit);
				
		// nasty way to access the services...feel free to have a better solution for this
		p_job.setServiceAccessor(getServiceAccessor());
		boolean success = m_job.pushJob(p_job);
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_submit);
		
		return success;
	}	
	
	/**
	 * Schedule a job for remote execution. The job is sent to the node specified and
	 * scheduled for exeuction there.
	 * @param p_job Job to schedule.
	 * @param p_nodeID ID of the node to schedule the job on.
	 * @return True of scheduling the job on the specified ID was successful, false otherwise.
	 */
	public boolean pushJobRemote(final Job p_job , final short p_nodeID)
	{
		// early return
		if (m_boot.getNodeRole() == NodeRole.SUPERPEER)
		{
			m_logger.error(getClass(), "A superpeer is not allowed to submit remote jobs.");
			return false;
		}
		
		boolean success = false;
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remoteSubmit);
		
		PushJobQueueRequest request = new PushJobQueueRequest(p_nodeID, p_job);
		NetworkErrorCodes error = m_network.sendSync(request);
		if (error != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending push job queue request to node " + p_nodeID + " failed: " + error);
		} else {
			PushJobQueueResponse response = request.getResponse(PushJobQueueResponse.class);
			byte statusCode = response.getStatusCode();
			if (statusCode != 0) {
				m_logger.error(getClass(), "Submitting remote job failed, remote node " + p_nodeID + " responded with error: " + statusCode);
			} else {
				success = true;
			}
		}
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remoteSubmit);
		
		return success;
	}
	
	/**
	 * Wait for all locally scheduled and currently executing jobs to finish.
	 * @return True if waiting was successful and all jobs finished, false otherwise.
	 */
	public boolean waitForLocalJobsToFinish()
	{
		return m_job.waitForSubmittedJobsToFinish();
	}
	
	// includes remote ones
	public boolean waitForAllJobsToFinish()
	{
		int successCount = 0;
		// if we checked all job systems successfully 
		// 5 times in a row, we consider this as a full termination
		while (successCount < 5)
		{
			// wait for local jobs to finish first
			if (!m_job.waitForSubmittedJobsToFinish())
				return false;
			
			// now check for remote peers
			List<Short> peers = m_boot.getAvailablePeerNodeIDs();
			for (short peer : peers)
			{
				// don't consider ourselves
				if (peer != m_boot.getNodeID())
				{
					StatusRequest request = new StatusRequest(peer);
					NetworkErrorCodes error = m_network.sendSync(request);
					if (error != NetworkErrorCodes.SUCCESS) {
						m_logger.error(getClass(), "Sending get status request to wait for termination to " + peer + " failed: " + error);
						// abort here as well, as we do not know what happened exactly
						return false;
					} else {
						StatusResponse response = request.getResponse(StatusResponse.class);
						if (response.getStatus().getNumberOfUnfinishedJobs() != 0)
						{
							successCount = 0;
							
							// not done, yet...sleep a little and try again
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e) {
							}
							continue;
						}
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
	public void onIncomingMessage(AbstractMessage p_message) {
		m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);

		if (p_message != null) {
			if (p_message.getType() == JobMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case JobMessages.SUBTYPE_PUSH_JOB_QUEUE_REQUEST:
					incomingPushJobQueueRequest((PushJobQueueRequest) p_message);
					break;
				case JobMessages.SUBTYPE_STATUS_REQUEST:
					incomingStatusRequest((StatusRequest) p_message);
					break;
				}
			}
		}

		m_logger.trace(getClass(), "Exiting incomingMessage");
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {

	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_boot = getComponent(BootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_job = getComponent(JobComponent.class);
		m_statistics = getComponent(StatisticsComponent.class);
		m_network = getComponent(NetworkComponent.class);
		
		registerNetworkMessages();
		registerNetworkMessageListener();
		registerStatisticsOperations();
		
		Job.registerType(JobNull.MS_TYPE_ID, JobNull.class);
		
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
	protected boolean isServiceAccessor()
	{
		return true;
	}
	
	// ------------------------------------------------------------------------------------------
	
	/**
	 * Register network messages used here.
	 */
	private void registerNetworkMessages()
	{
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_REQUEST, PushJobQueueRequest.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_RESPONSE, PushJobQueueResponse.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_STATUS_REQUEST, StatusRequest.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_STATUS_RESPONSE, StatusResponse.class);
	}
	
	/**
	 * Register listener for incoming messages.
	 */
	private void registerNetworkMessageListener()
	{
		m_network.register(PushJobQueueRequest.class, this);
		m_network.register(StatusRequest.class, this);
	}
	
	/**
	 * Register statistics stuff.
	 */
	private void registerStatisticsOperations() 
	{
		m_statisticsRecorderIDs = new JobStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(this.getClass());
		
		m_statisticsRecorderIDs.m_operations.m_submit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_SUBMIT);
		m_statisticsRecorderIDs.m_operations.m_remoteSubmit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_REMOTE_SUBMIT);
		m_statisticsRecorderIDs.m_operations.m_incomingSubmit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_INCOMING_SUBMIT);
	}

	/**
	 * Handle incoming push queue request.
	 * @param p_request Incoming request.
	 */
	private void incomingPushJobQueueRequest(final PushJobQueueRequest p_request)
	{
		boolean success = false;
	
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingSubmit);
		
		Job job = p_request.getJob();
		job.setServiceAccessor(getServiceAccessor());
		
		m_job.pushJob(job);
		
		PushJobQueueResponse response;
		if (success)
			response = new PushJobQueueResponse(p_request, (byte) 0);
		else
			response = new PushJobQueueResponse(p_request, (byte) -1);
		
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending PushJobQueueResponse for " + p_request.getJob() + " failed: " + error);
		}
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingSubmit);
	}
	
	/**
	 * Handle incoming status request.
	 * @param p_request Incoming request.
	 */
	private void incomingStatusRequest(final StatusRequest p_request)
	{
		Status status = new Status();
		status.m_numUnfinishedJobs = m_job.getNumberOfUnfinishedJobs();
		
		StatusResponse response = new StatusResponse(p_request, status);
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending StatusResponse for " + p_request + " failed: " + error);
		}
	}
	
	public static class Status implements Importable, Exportable
	{
		private long m_numUnfinishedJobs = -1;
		
		public Status()
		{
			
		}
		
		public long getNumberOfUnfinishedJobs() 
		{
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
		public int exportObject(Exporter p_exporter, int p_size) {
			p_exporter.writeLong(m_numUnfinishedJobs);
			return sizeofObject();
		}

		@Override
		public int importObject(Importer p_importer, int p_size) {
			m_numUnfinishedJobs = p_importer.readLong();
			return sizeofObject();
		}
	}
}
