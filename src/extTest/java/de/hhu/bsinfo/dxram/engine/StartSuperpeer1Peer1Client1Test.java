package de.hhu.bsinfo.dxram.engine;

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
public class StartSuperpeer1Peer1Client1Test {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void test() {
        BootService bootService = m_instance.getService(BootService.class);
        Assert.assertEquals(NodeCapabilities.NONE, bootService.getNodeCapabilities(bootService.getNodeID()));
        Assert.assertEquals(1, bootService.getSupportingNodes(NodeCapabilities.STORAGE).size());
    }
}
