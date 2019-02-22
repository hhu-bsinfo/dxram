package de.hhu.bsinfo.dxram.function;

import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.function.util.ParameterList;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ClassUtil;

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
        FunctionService.Status status = functionService.register(peer, IntAdderFunction.NAME, function);
        assertEquals(FunctionService.Status.REGISTERED, status);

        IntAdderFunction.Input input;
        IntAdderFunction.Output output;

        input = new IntAdderFunction.Input(17, 25);
        output = functionService.executeSync(peer, IntAdderFunction.NAME, input);

        assertEquals(42, output.get());

        input = new IntAdderFunction.Input(1, 2, 3, 4, 5, 6, 7, 8);
        output = functionService.executeSync(peer, IntAdderFunction.NAME, input);
        assertEquals(36, output.get());
    }
}
