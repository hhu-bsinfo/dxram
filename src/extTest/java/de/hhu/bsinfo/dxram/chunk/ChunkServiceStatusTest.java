package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.chunk.data.ChunkServiceStatus;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceStatusTest {
    @TestInstance(runOnNodeIdx = 2)
    public void test(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(p_instance);

        ChunkServiceStatus statusLocal = chunkService.status().getStatus();
        ChunkServiceStatus statusRemote = chunkService.status().getStatus(remotePeer);

        // one chunk allocated (index chunk)
        assertServiceStatus(statusLocal);
        assertServiceStatus(statusRemote);
    }

    private void assertServiceStatus(final ChunkServiceStatus p_status) {
        Assert.assertEquals(6, p_status.getHeapStatus().getAllocatedBlocks());
        Assert.assertEquals(1, p_status.getHeapStatus().getFreeBlocks());
        Assert.assertEquals(0, p_status.getHeapStatus().getFreeSmall64ByteBlocks());

        Assert.assertEquals(0, p_status.getLIDStoreStatus().getCurrentLIDCounter());
        Assert.assertEquals(0, p_status.getLIDStoreStatus().getTotalFreeLIDs());
        Assert.assertEquals(0, p_status.getLIDStoreStatus().getTotalLIDsInStore());

        Assert.assertEquals(5, p_status.getCIDTableStatus().getTotalTableCount());
        Assert.assertEquals(1, p_status.getCIDTableStatus().getTableCountOfLevel(3));
        Assert.assertEquals(1, p_status.getCIDTableStatus().getTableCountOfLevel(2));
        Assert.assertEquals(1, p_status.getCIDTableStatus().getTableCountOfLevel(1));
        Assert.assertEquals(1, p_status.getCIDTableStatus().getTableCountOfLevel(0));
    }
}
