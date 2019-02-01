package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceGetAnonTest {
    @TestInstance(runOnNodeIdx = 2)
    public void getSimple(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        ChunkAnonService chunkAnonService = p_instance.getService(ChunkAnonService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(p_instance);

        long[] cids = new long[1];
        int ret = chunkService.create().create(remotePeer, cids, 1, ChunkTestConstants.CHUNK_SIZE_4);

        Assert.assertEquals(1, ret);
        Assert.assertNotEquals(ChunkID.INVALID_ID, cids[0]);
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(cids[0]));

        ChunkAnon[] chunks = new ChunkAnon[1];

        ret = chunkAnonService.getAnon().get(chunks, cids);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunks[0].isIDValid());
        Assert.assertTrue(chunks[0].isStateOk());
    }
}
