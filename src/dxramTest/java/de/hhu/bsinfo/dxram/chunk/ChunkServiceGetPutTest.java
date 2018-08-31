package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceGetPutTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void getSimple() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(m_instance);

        ChunkByteArray chunk = new ChunkByteArray(ChunkTestConstants.CHUNK_SIZE_1);
        int ret = chunkService.create().create(remotePeer, chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(chunk.getID()));

        ret = chunkService.get().get(chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
    }

    @Test
    public void putGetSimple() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(m_instance);

        ChunkByteArray chunk = new ChunkByteArray(ChunkTestConstants.CHUNK_SIZE_1);
        int ret = chunkService.create().create(remotePeer, chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(chunk.getID()));

        chunk.getData()[0] = (byte) 0xAA;

        ret = chunkService.put().put(chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());

        ret = chunkService.get().get(chunk);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunk.isIDValid());
        Assert.assertTrue(chunk.isStateOk());
        Assert.assertEquals((byte) 0xAA, chunk.getData()[0]);
    }
}
