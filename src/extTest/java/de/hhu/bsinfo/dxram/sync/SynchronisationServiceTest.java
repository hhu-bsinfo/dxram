package de.hhu.bsinfo.dxram.sync;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 1,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class SynchronisationServiceTest {
    private static final long CUSTOM_DATA = 0x12345;

    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void simpleTest() {
        SynchronizationService syncService = m_instance.getService(SynchronizationService.class);

        int barrierId = syncService.barrierAllocate(1);

        BarrierStatus status = syncService.barrierGetStatus(barrierId);

        Assert.assertEquals(0, status.getNumberOfSignedOnPeers());
        Assert.assertEquals(1, status.getCustomData().length);
        Assert.assertEquals(1, status.getSignedOnNodeIDs().length);

        status = syncService.barrierSignOn(barrierId, CUSTOM_DATA, true);

        Assert.assertEquals(1, status.getNumberOfSignedOnPeers());
        Assert.assertEquals(CUSTOM_DATA, status.getCustomData()[0]);

        syncService.barrierFree(barrierId);
    }
}
