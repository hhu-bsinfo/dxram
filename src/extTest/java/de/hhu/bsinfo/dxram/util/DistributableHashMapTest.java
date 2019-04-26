package de.hhu.bsinfo.dxram.util;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkTestConstants;
import de.hhu.bsinfo.dxram.chunk.ChunkTestUtils;
import de.hhu.bsinfo.dxram.function.DistributableFunction;
import de.hhu.bsinfo.dxram.function.FunctionService;
import de.hhu.bsinfo.dxram.function.IntAdderFunction;
import de.hhu.bsinfo.dxram.function.MapInverterFunction;
import de.hhu.bsinfo.dxutils.NodeID;

import static org.junit.Assert.assertEquals;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class DistributableHashMapTest {

    @TestInstance(runOnNodeIdx = 2)
    public void testDistributableHashMap(final DXRAM p_instance) {

        DistributableHashMap<String, Integer> hashMap =
                new DistributableHashMap<>(DistributableString::new, DistributableInteger::new);

        FunctionService functionService = p_instance.getService(FunctionService.class);
        BootService bootService = p_instance.getService(BootService.class);

        short peer = bootService.getOnlinePeerNodeIDs()
                .stream()
                .filter(p_id -> p_id != bootService.getNodeID())
                .findFirst().orElse(NodeID.INVALID_ID);

        Assert.assertNotEquals(NodeID.INVALID_ID, peer);

        DistributableFunction function = new MapInverterFunction();
        FunctionService.Status status = functionService.register(peer, MapInverterFunction.NAME, function);
        assertEquals(FunctionService.Status.REGISTERED, status);

        DistributableHashMap<String, Integer> input;
        DistributableHashMap<Integer, String> output;

        input = new DistributableHashMap<>(DistributableString::new, DistributableInteger::new);

        input.put("A", 1);
        input.put("B", 2);
        input.put("C", 3);

        output = functionService.executeSync(peer, MapInverterFunction.NAME, input);

        assertEquals("A", output.get(1));
        assertEquals("B", output.get(2));
        assertEquals("C", output.get(3));

    }
}