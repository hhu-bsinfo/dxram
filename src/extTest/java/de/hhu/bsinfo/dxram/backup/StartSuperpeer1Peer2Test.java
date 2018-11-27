package de.hhu.bsinfo.dxram.backup;

import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 1,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, backupActive = true, availableForBackup = true),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, backupActive = true, availableForBackup = true)
        })
public class StartSuperpeer1Peer2Test {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void test() {

    }
}
