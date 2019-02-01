package de.hhu.bsinfo.dxram.job;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, enableJobService = true)
        })
public class JobServiceTest {
    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) {
        JobService jobService = p_instance.getService(JobService.class);

        JobTest job = new JobTest(5);

        long jobId = jobService.pushJob(job);
        Assert.assertNotEquals(JobID.INVALID_ID, jobId);

        Assert.assertTrue(jobService.waitForLocalJobsToFinish());

        Assert.assertEquals(10, job.getValue());
    }
}
