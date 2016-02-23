package de.hhu.bsinfo.dxram.run.nothaas;

import java.util.Random;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

public class SimpleJobServiceTest extends Main {

	public static class JobTest extends Job
	{
		public static final short MS_TYPE_ID = 1;
		
		public JobTest()
		{
			super();
		}
		
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
	
	private static final Argument ARG_REMOTE_PEER = new Argument("remotePeer", true, true, "Indicates if this is the remote peer waiting for other jobs to receive");
	private static final Argument ARG_NUM_JOBS = new Argument("numJobs", 10, true, "Number of jobs to create for testing");
	private static final Argument ARG_NUM_REMOTE_JOBS = new Argument("numRemoteJobs", 0, true, "Number of remote jobs to create");
	private static final Argument ARG_REMOTE_NODE = new Argument("remoteNode", null, false, "Node ID of the remote node to send remote jobs to");
	
	private DXRAM m_dxram = null;
	private JobService m_jobService = null;
	
	public static void main(final String[] args) {
		Main main = new SimpleJobServiceTest();
		main.run(args);
	}
	
	public SimpleJobServiceTest() 
	{
		super("Testing the JobService and its remote job execution");
		
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf", true);
		m_jobService = m_dxram.getService(JobService.class);
		m_jobService.registerJobType(JobTest.MS_TYPE_ID, JobTest.class);
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
					m_jobService.pushJobRemote(new JobTest(ran.nextInt(10) * 500), remoteNode);
				} else {
					m_jobService.pushJob(new JobTest(ran.nextInt(10) * 500));
				}
			}
			
			m_jobService.waitForLocalJobsToFinish();
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
