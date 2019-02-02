package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkWriteLockTest {
    @TestInstance(runOnNodeIdx = 2)
    public void writeLock(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(p_instance);

        ChunkByteArray chunk = new ChunkByteArray(ChunkTestConstants.CHUNK_SIZE_1);
        int ret = chunkService.create().create(remotePeer, chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(chunk.getID()));

        ret = chunkService.lock().lock(true, true, -1, chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());

        ret = chunkService.lock().lock(false, true, -1, chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());

        ret = chunkService.remove().remove(chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());

        // wait a moment because remove "jobs" are dispatched by another thread
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

        ChunkIDRanges range = chunkService.cidStatus().getAllLocalChunkIDRanges(remotePeer);
        Assert.assertFalse(range.isInRange(chunk.getID()));
    }
}
