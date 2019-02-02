package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                // simulate a client without any backend storage
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, enableKeyValueStorage = false)
        })
public class ChunkServiceClientRemoteTest {
    @TestInstance(runOnNodeIdx = 2)
    public void test(final DXRAM p_instance) {
        BootService bootService = p_instance.getService(BootService.class);
        Assert.assertEquals(NodeCapabilities.NONE, bootService.getNodeCapabilities(bootService.getNodeID()));

        TestChunk chunk = new TestChunk(true);

        ChunkService chunkService = p_instance.getService(ChunkService.class);

        try {
            chunkService.create().create(bootService.getNodeID(), chunk);
        } catch (final IllegalStateException e) {
            return;
        }

        Assert.fail("No illegal access due to disabled chunk storage thrown.");
    }
}
