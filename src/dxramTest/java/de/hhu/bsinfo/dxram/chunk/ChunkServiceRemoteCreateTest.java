package de.hhu.bsinfo.dxram.chunk;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMRunnerConfiguration;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMRunnerConfiguration(runTestOnNodeIdx = 2,
        nodes = {
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class ChunkServiceRemoteCreateTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void create() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = getRemotePeer();

        int[] sizes = new int[] {64, 128, 321, 932, 999};
        long[] chunkIds = chunkService.createRemote(remotePeer, sizes);

        for (int i = 0; i < sizes.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, chunkIds[i]);
        }
    }

    @Test
    public void create2() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        short remotePeer = getRemotePeer();

        TestChunk[] chunks = new TestChunk[15];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new TestChunk(false);
        }

        int count = chunkService.createRemote(remotePeer, chunks);

        Assert.assertEquals(chunks.length, count);

        for (int i = 0; i < chunks.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, chunks[i].getID());
            Assert.assertEquals(ChunkState.OK, chunks[i].getState());
        }
    }

    private short getRemotePeer() {
        BootService bootService = m_instance.getService(BootService.class);

        List<Short> peers = bootService.getOnlinePeerNodeIDs();

        for (short peer : peers) {
            if (peer != bootService.getNodeID()) {
                return peer;
            }
        }

        throw new IllegalStateException("Could not find other peer");
    }
}
