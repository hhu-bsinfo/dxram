package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                // simulate a client without any backend storage
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, enableKeyValueStorage = false)
        })
public class ChunkServiceClientRemote2Test {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void test() {
        BootService bootService = m_instance.getService(BootService.class);
        Assert.assertEquals(NodeCapabilities.NONE, bootService.getNodeCapabilities(bootService.getNodeID()));

        TestChunk chunk = new TestChunk(true);

        ChunkService chunkService = m_instance.getService(ChunkService.class);
        int success = chunkService.create().create(bootService.getSupportingNodes(NodeCapabilities.STORAGE).get(0),
                chunk);
        Assert.assertEquals(1, success);
    }
}
