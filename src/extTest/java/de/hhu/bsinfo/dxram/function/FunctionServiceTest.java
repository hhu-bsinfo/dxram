package de.hhu.bsinfo.dxram.function;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.BeforeTestInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.job.JobID;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.job.JobTest;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

import static org.junit.Assert.assertEquals;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class FunctionServiceTest {

    private static final String FUNCTION_NAME = "de.hhu.bsinfo.dxram.hello";

    @TestInstance(runOnNodeIdx = 2)
    public void testRemoteExecution(final DXRAM p_instance) {

        FunctionService functionService = p_instance.getService(FunctionService.class);
        BootService bootService = p_instance.getService(BootService.class);

        short peer = bootService.getOnlinePeerNodeIDs()
                .stream()
                .filter(p_id -> p_id != bootService.getNodeID())
                .findFirst().orElseGet(() -> NodeID.INVALID_ID);

        Assert.assertNotEquals(NodeID.INVALID_ID, peer);

        DistributableFunction function = p_serviceAccessor -> {
            BootService boot = p_serviceAccessor.getService(BootService.class);
            System.out.printf("Hello, I am node %04X\n", boot.getNodeID());
        };

        FunctionService.Status status = functionService.registerFunction(peer, FUNCTION_NAME, function);

        assertEquals(FunctionService.Status.REGISTERED, status);

        functionService.executeFunction(peer, FUNCTION_NAME);

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
    }
}
