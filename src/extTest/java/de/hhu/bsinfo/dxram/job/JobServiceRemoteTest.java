package de.hhu.bsinfo.dxram.job;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, enableJobService = true)
        })
public class JobServiceRemoteTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void remoteTest() {
        JobService jobService = m_instance.getService(JobService.class);
        BootService bootService = m_instance.getService(BootService.class);

        List<Short> peers = bootService.getOnlinePeerNodeIDs();
        short otherPeer = NodeID.INVALID_ID;

        for (short peer : peers) {
            if (peer != bootService.getNodeID()) {
                otherPeer = peer;
                break;
            }
        }

        Assert.assertNotEquals(NodeID.INVALID_ID, otherPeer);

        JobTest job = new JobTest(5);

        long jobId = jobService.pushJobRemote(job, otherPeer);
        Assert.assertNotEquals(JobID.INVALID_ID, jobId);

        Assert.assertTrue(jobService.waitForRemoteJobsToFinish());
    }
}
