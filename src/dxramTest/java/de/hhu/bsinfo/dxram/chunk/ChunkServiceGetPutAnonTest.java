package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceGetPutAnonTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void getSimple() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        ChunkAnonService chunkAnonService = m_instance.getService(ChunkAnonService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(m_instance);

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

    @Test
    public void putGetSimple() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        ChunkAnonService chunkAnonService = m_instance.getService(ChunkAnonService.class);
        short remotePeer = ChunkTestUtils.getRemotePeer(m_instance);

        long[] cids = new long[1];
        int ret = chunkService.create().create(remotePeer, cids, 1, ChunkTestConstants.CHUNK_SIZE_4);

        Assert.assertEquals(1, ret);
        Assert.assertNotEquals(ChunkID.INVALID_ID, cids[0]);
        Assert.assertEquals(remotePeer, ChunkID.getCreatorID(cids[0]));

        ChunkAnon[] chunks = new ChunkAnon[1];
        chunks[0] = new ChunkAnon(cids[0], new byte[ChunkTestConstants.CHUNK_SIZE_4]);
        chunks[0].getData()[0] = (byte) 0xAA;

        ret = chunkAnonService.putAnon().put(chunks);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunks[0].isIDValid());
        Assert.assertTrue(chunks[0].isStateOk());

        ret = chunkAnonService.getAnon().get(chunks, cids);

        Assert.assertEquals(1, ret);
        Assert.assertTrue(chunks[0].isIDValid());
        Assert.assertTrue(chunks[0].isStateOk());
        Assert.assertEquals(chunks[0].getData()[0], (byte) 0xAA);
    }
}
