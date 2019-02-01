package de.hhu.bsinfo.dxram.sync;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class SynchronisationServiceTest {
    private static final int NUM_PEERS_SIGN_ON = 4;
    private static final String NAMESERVICE_STR = "BAR0";
    private static final long CUSTOM_DATA = 0x12345;

    @TestInstance(runOnNodeIdx = 1)
    public void master(final DXRAM p_instance) {
        SynchronizationService syncService = p_instance.getService(SynchronizationService.class);
        NameserviceService nameservice = p_instance.getService(NameserviceService.class);

        int barrierId = syncService.barrierAllocate(NUM_PEERS_SIGN_ON);

        BarrierStatus status = syncService.barrierGetStatus(barrierId);

        Assert.assertEquals(0, status.getNumberOfSignedOnPeers());
        Assert.assertEquals(NUM_PEERS_SIGN_ON, status.getCustomData().length);
        Assert.assertEquals(NUM_PEERS_SIGN_ON, status.getSignedOnNodeIDs().length);

        nameservice.register(barrierId, NAMESERVICE_STR);

        status = syncService.barrierSignOn(barrierId, CUSTOM_DATA, true);

        Assert.assertEquals(NUM_PEERS_SIGN_ON, status.getNumberOfSignedOnPeers());

        for (int i = 0; i < NUM_PEERS_SIGN_ON; i++) {
            Assert.assertEquals(CUSTOM_DATA, status.getCustomData()[i]);
        }

        syncService.barrierFree(barrierId);
    }

    @TestInstance(runOnNodeIdx = {2, 3, 4})
    public void slave(final DXRAM p_instance) {
        SynchronizationService syncService = p_instance.getService(SynchronizationService.class);
        NameserviceService nameservice = p_instance.getService(NameserviceService.class);

        int barrierId = BarrierID.INVALID_ID;

        while (barrierId == BarrierID.INVALID_ID) {
            barrierId = (int) nameservice.getChunkID(NAMESERVICE_STR, 1000);
        }

        BarrierStatus status = syncService.barrierSignOn(barrierId, CUSTOM_DATA, true);

        Assert.assertEquals(NUM_PEERS_SIGN_ON, status.getNumberOfSignedOnPeers());

        for (int i = 0; i < NUM_PEERS_SIGN_ON; i++) {
            Assert.assertEquals(CUSTOM_DATA, status.getCustomData()[i]);
        }
    }
}
