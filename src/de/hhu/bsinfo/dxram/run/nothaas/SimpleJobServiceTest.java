
package de.hhu.bsinfo.dxram.run.nothaas;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxcompute.job.AbstractJob;
import de.hhu.bsinfo.dxcompute.job.JobService;
import de.hhu.bsinfo.dxcompute.job.event.JobEventListener;
import de.hhu.bsinfo.dxcompute.job.event.JobEvents;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Test of the JobService.
 * Run this as a peer, start one superpeer and an additional
 * peer service as remote instancing receiving remote jobs if this
 * options was selected.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class SimpleJobServiceTest extends AbstractMain implements JobEventListener {

	private static final Logger LOGGER = LogManager.getFormatterLogger(SimpleJobServiceTest.class.getSimpleName());

	private static final Argument ARG_REMOTE_PEER = new Argument("remotePeer", "true", true,
			"Indicates if this is the remote peer waiting for other jobs to receive");
	private static final Argument ARG_NUM_JOBS =
			new Argument("numJobs", "10", true, "Number of jobs to create for testing");
	private static final Argument ARG_NUM_REMOTE_JOBS =
			new Argument("numRemoteJobs", "0", true, "Number of remote jobs to create");
	private static final Argument ARG_REMOTE_NODE =
			new Argument("remoteNode", null, false, "Node ID of the remote node to send remote jobs to");

	private DXRAM m_dxram;
	private JobService m_jobService;
	private BootService m_bootService;

	private AtomicInteger m_remoteJobCount = new AtomicInteger(0);

	/**
	 * Constructor
	 */
	public SimpleJobServiceTest() {
		super("Testing the JobService and its remote job execution");

		m_dxram = new DXRAM();
		m_dxram.initialize(true);
		m_jobService = m_dxram.getService(JobService.class);
		m_bootService = m_dxram.getService(BootService.class);
		m_jobService.registerJobType(JobTest.MS_TYPE_ID, JobTest.class);
	}

	/**
	 * Java main entry point.
	 *
	 * @param p_args Main arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new SimpleJobServiceTest();
		main.run(p_args);
	}

	@Override
	public byte getJobEventBitMask() {
		return JobEvents.MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID
				| JobEvents.MS_JOB_STARTED_EXECUTION_EVENT_ID
				| JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID;
	}

	@Override
	public void jobEventTriggered(final byte p_eventId, final long p_jobId, final short p_sourceNodeId) {
		System.out.println("JobEvent: " + p_eventId + " | " + Long.toHexString(p_jobId) + " | "
				+ NodeID.toHexString(p_sourceNodeId));
		if (p_eventId == JobEvents.MS_JOB_FINISHED_EXECUTION_EVENT_ID && p_sourceNodeId != m_bootService.getNodeID()) {
			m_remoteJobCount.decrementAndGet();
		}
	}

	@Override
	protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {

	}

	@Override
	protected int main(final ArgumentList p_arguments) {
		boolean remotePeer = p_arguments.getArgument(ARG_REMOTE_PEER).getValue(Boolean.class);
		int numJobs = p_arguments.getArgument(ARG_NUM_JOBS).getValue(Integer.class);
		int numRemoteJobs = p_arguments.getArgument(ARG_NUM_REMOTE_JOBS).getValue(Integer.class);
		Short remoteNode = p_arguments.getArgument(ARG_REMOTE_NODE).getValue(Short.class);

		if (!remotePeer) {
			if (numJobs < numRemoteJobs) {
				numRemoteJobs = numJobs;
			}

			Random ran = new Random();
			for (int i = 0; i < numJobs; i++) {
				if (remoteNode != -1 && numRemoteJobs > 0) {
					numRemoteJobs--;
					m_remoteJobCount.incrementAndGet();
					AbstractJob job = new JobTest(ran.nextInt(10) * 500);
					job.registerEventListener(this);
					m_jobService.pushJobRemote(job, remoteNode);
				} else {
					AbstractJob job = new JobTest(ran.nextInt(10) * 500);
					job.registerEventListener(this);
					m_jobService.pushJob(job);
				}
			}

			m_jobService.waitForLocalJobsToFinish();

			while (m_remoteJobCount.get() > 0) {
				Thread.yield();
			}

			System.out.println("All jobs finished.");
		} else {
			while (true) {

			}
		}

		return 0;
	}

	/**
	 * Implementation of a job for this test.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
	 */
	public static class JobTest extends AbstractJob {
		public static final short MS_TYPE_ID = 1;

		/**
		 * Constructor
		 */
		public JobTest() {
			super();
		}

		/**
		 * Constructor
		 *
		 * @param p_parameterChunkIDs ChunkIDs to pass to the job.
		 */
		public JobTest(final long... p_parameterChunkIDs) {
			super(p_parameterChunkIDs);
		}

		@Override
		public short getTypeID() {
			return MS_TYPE_ID;
		}

		@Override
		protected void execute(final short p_nodeID, final long[] p_chunkIDs) {
			LoggerService logger = getService(LoggerService.class);
			try {
				// abusing chunkID for time to wait
				// #if LOGGER >= DEBUG
				LOGGER.debug("Sleeping %d", p_chunkIDs[0]);
				// #endif /* LOGGER >= DEBUG */

				Thread.sleep(p_chunkIDs[0]);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
