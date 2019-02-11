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
import de.hhu.bsinfo.dxram.function.util.ParameterList;
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

    @TestInstance(runOnNodeIdx = 2)
    public void testRemoteExecution(final DXRAM p_instance) {

        FunctionService functionService = p_instance.getService(FunctionService.class);
        BootService bootService = p_instance.getService(BootService.class);

        short peer = bootService.getOnlinePeerNodeIDs()
                .stream()
                .filter(p_id -> p_id != bootService.getNodeID())
                .findFirst().orElse(NodeID.INVALID_ID);

        Assert.assertNotEquals(NodeID.INVALID_ID, peer);

        DistributableFunction function = new IntAdderFunction();
        FunctionService.Status status = functionService.registerFunction(peer, IntAdderFunction.NAME, function);
        assertEquals(FunctionService.Status.REGISTERED, status);

        ParameterList params, result;

        params = new ParameterList(new String[]{"17", "25"});
        result = functionService.executeFunctionSync(peer, IntAdderFunction.NAME, params);

        assertEquals("42", result.get(0));

        params = new ParameterList(new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});
        result = functionService.executeFunctionSync(peer, IntAdderFunction.NAME, params);
        assertEquals("36", result.get(0));
    }
}
