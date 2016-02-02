package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.job.messages.JobMessages;
import de.hhu.bsinfo.dxram.job.messages.PushJobQueueRequest;
import de.hhu.bsinfo.dxram.job.messages.PushJobQueueResponse;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent.ErrorCode;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

public class JobService extends DXRAMService implements MessageReceiver, RemoteSubmissionDelegate {

	private BootComponent m_boot = null;
	private LoggerComponent m_logger = null;
	private JobComponent m_job = null;
	private StatisticsComponent m_statistics = null;
	private NetworkComponent m_network = null;
	
	private JobStatisticsRecorderIDs m_statisticsRecorderIDs = null;
	
	public void registerJobType(final short p_typeID, final Class<? extends Job> p_clazz)
	{
		m_logger.debug(getClass(), "Registering job type " + p_typeID + " for class " + p_clazz);
		Job.registerType(p_typeID, p_clazz);
	}
	
	public boolean pushJob(final Job p_job)
	{
		// early return
		if (m_boot.getNodeRole() == NodeRole.SUPERPEER)
		{
			m_logger.error(getClass(), "A superpeer is not allowed to submit jobs.");
			return false;
		}
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_submit);
				
		boolean success = m_job.pushJob(p_job);
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_submit);
		
		return success;
	}	
	
	@Override
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
		ErrorCode error = m_network.sendSync(request);
		if (error != ErrorCode.SUCCESS) {
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
	
	public boolean waitForLocalJobsToFinish()
	{
		return m_job.waitForSubmittedJobsToFinish();
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
		
		m_job.setRemoteSubsmissionDelegate(this);
		
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
	
	// ------------------------------------------------------------------------------------------
	
	private void registerNetworkMessages()
	{
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_REQUEST, PushJobQueueRequest.class);
		m_network.registerMessageType(JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_RESPONSE, PushJobQueueResponse.class);
	}
	
	private void registerNetworkMessageListener()
	{
		m_network.register(PushJobQueueRequest.class, this);
	}
	
	private void registerStatisticsOperations() 
	{
		m_statisticsRecorderIDs = new JobStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(this.getClass());
		
		m_statisticsRecorderIDs.m_operations.m_submit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_SUBMIT);
		m_statisticsRecorderIDs.m_operations.m_remoteSubmit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_REMOTE_SUBMIT);
		m_statisticsRecorderIDs.m_operations.m_incomingSubmit = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, JobStatisticsRecorderIDs.Operations.MS_INCOMING_SUBMIT);
	}

	private void incomingPushJobQueueRequest(final PushJobQueueRequest p_request)
	{
		boolean success = false;
	
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingSubmit);
		
		m_job.pushJob(p_request.getJob());
		
		PushJobQueueResponse response;
		if (success)
			response = new PushJobQueueResponse(p_request, (byte) 0);
		else
			response = new PushJobQueueResponse(p_request, (byte) -1);
		
		ErrorCode error = m_network.sendMessage(response);
		if (error != ErrorCode.SUCCESS)
		{
			m_logger.error(getClass(), "Sending PushJobQueueResponse for " + p_request.getJob() + " failed: " + error);
		}
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingSubmit);
	}
}
