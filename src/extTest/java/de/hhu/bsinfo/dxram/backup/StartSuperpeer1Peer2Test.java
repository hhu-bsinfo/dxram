package de.hhu.bsinfo.dxram.backup;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, backupActive = true, availableForBackup = true),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, backupActive = true, availableForBackup = true)
        })
public class StartSuperpeer1Peer2Test {
    @TestInstance(runOnNodeIdx = 1)
    public void test(final DXRAM p_instance) {

    }
}
