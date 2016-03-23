package de.hhu.bsinfo.dxram.run.nothaas;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.job.event.JobEventListener;
import de.hhu.bsinfo.dxram.job.event.JobEvents;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Test of the JobService. 
 * Run this as a peer, start one superpeer and an additional 
 * peer service as remote instancing receiving remote jobs if this
 * options was selected.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class SimpleJobServiceTest extends Main implements JobEventListener {

	/**
	 * Implementation of a job for this test.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
	 *
	 */
	public static class JobTest extends Job
	{
		public static final short MS_TYPE_ID = 1;
		
		/**
		 * Constructor
		 */
		public JobTest()
		{
			super();
		}
		
		/**
		 * Constructor
		 * @param p_parameterChunkIDs ChunkIDs to pass to the job.
		 */
		public JobTest(final long... p_parameterChunkIDs)
		{
			super(p_parameterChunkIDs);
		}
		
		@Override
		public short getTypeID() {
			return MS_TYPE_ID;
		}

		@Override
		protected void execute(short p_nodeID, long[] p_chunkIDs) {
			LoggerService logger = getService(LoggerService.class);
			try {
				// abusing chunkID for time to wait
				logger.debug(getClass(), "Sleeping " + p_chunkIDs[0]);
				Thread.sleep(p_chunkIDs[0]);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static final Argument ARG_REMOTE_PEER = new Argument("remotePeer", "true", true, "Indicates if this is the remote peer waiting for other jobs to receive");
	private static final Argument ARG_NUM_JOBS = new Argument("numJobs", "10", true, "Number of jobs to create for testing");
	private static final Argument ARG_NUM_REMOTE_JOBS = new Argument("numRemoteJobs", "0", true, "Number of remote jobs to create");
	private static final Argument ARG_REMOTE_NODE = new Argument("remoteNode", null, false, "Node ID of the remote node to send remote jobs to");
	
	private DXRAM m_dxram = null;
	private JobService m_jobService = null;
	private BootService m_bootService = null;
	
	private AtomicInteger m_remoteJobCount = new AtomicInteger(0);
	
	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		Main main = new SimpleJobServiceTest();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	public SimpleJobServiceTest() 
	{
		super("Testing the JobService and its remote job execution");
		
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf", true);
		m_jobService = m_dxram.getService(JobService.class);
		m_bootService = m_dxram.getService(BootService.class);
		m_jobService.registerJobType(JobTest.MS_TYPE_ID, JobTest.class);
	}
	
	@Override
	public byte getJobEventBitMask() {
		return JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID |
				JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID |
				JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID;
	}

	@Override
	public void jobEventTriggered(byte p_eventId, long p_jobId, short p_sourceNodeId) 
	{
		System.out.println("JobEvent: " + p_eventId + " | " + Long.toHexString(p_jobId) + " | " + Integer.toHexString(p_sourceNodeId));
		if (p_eventId == JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID && p_sourceNodeId != m_bootService.getNodeID()) {
			m_remoteJobCount.decrementAndGet();
		}
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {

	}

	@Override
	protected int main(ArgumentList p_arguments) {
		boolean remotePeer = p_arguments.getArgument(ARG_REMOTE_PEER).getValue(Boolean.class);
		int numJobs = p_arguments.getArgument(ARG_NUM_JOBS).getValue(Integer.class);
		int numRemoteJobs = p_arguments.getArgument(ARG_NUM_REMOTE_JOBS).getValue(Integer.class);
		Short remoteNode = p_arguments.getArgument(ARG_REMOTE_NODE).getValue(Short.class);
		
		if (!remotePeer)
		{
			if (numJobs < numRemoteJobs) {
				numRemoteJobs = numJobs;
			}
			
			Random ran = new Random();
			for (int i = 0; i < numJobs; i++)
			{
				if (remoteNode != -1 && numRemoteJobs > 0) {
					numRemoteJobs--;
					m_remoteJobCount.incrementAndGet();
					Job job = new JobTest(ran.nextInt(10) * 500);
					job.registerEventListener(this);
					m_jobService.pushJobRemote(job, remoteNode);
				} else {
					Job job = new JobTest(ran.nextInt(10) * 500);
					job.registerEventListener(this);
					m_jobService.pushJob(job);
				}
			}
			
			m_jobService.waitForLocalJobsToFinish();
			
			while (m_remoteJobCount.get() > 0)
			{
				Thread.yield();
			}
			
			System.out.println("All jobs finished.");
		}
		else
		{
			while (true)
			{
				
			}
		}

		return 0;
	}
}
