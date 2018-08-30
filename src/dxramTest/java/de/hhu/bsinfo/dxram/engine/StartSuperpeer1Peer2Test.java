package de.hhu.bsinfo.dxram.engine;

import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMRunnerConfiguration;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMRunnerConfiguration(runTestOnNodeIdx = 1,
        nodes = {
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class StartSuperpeer1Peer2Test {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void test() {

    }
}
