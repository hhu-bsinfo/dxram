package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkResizeTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void simple() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(m_instance);

        ChunkByteArray chunk = new ChunkByteArray(ChunkTestConstants.CHUNK_SIZE_1);
        int ret = chunkService.create().create(remotePeer, chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(chunk.getID()));

        ChunkByteArray chunkNewSize = new ChunkByteArray(ChunkTestConstants.CHUNK_SIZE_2);
        chunkNewSize.setID(chunk.getID());

        ret = chunkService.resize().resize(chunkNewSize);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(chunk.getID()));

        ret = chunkService.remove().remove(chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());

        // wait a moment because remove "jobs" are dispatched by another thread
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ChunkIDRanges range = chunkService.cidStatus().getAllLocalChunkIDRanges(remotePeer);
        Assert.assertFalse(range.isInRange(chunk.getID()));
    }
}
