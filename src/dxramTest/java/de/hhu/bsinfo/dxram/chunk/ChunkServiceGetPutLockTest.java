package de.hhu.bsinfo.dxram.chunk;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

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
public class ChunkServiceGetPutLockTest {
    @ClientInstance
    private DXRAM m_instance;

    private ChunkIDRanges m_cidRanges;

    @BeforeClass
    public void setup() {
        m_cidRanges = new ChunkIDRanges();
        ChunkTestUtils.createChunks(m_instance, m_cidRanges, false, ChunkTestConstants.CHUNK_SIZE_2, 1000);
        ChunkTestUtils.createChunks(m_instance, m_cidRanges, true, ChunkTestConstants.CHUNK_SIZE_2, 1000);
    }

    @Test
    public void test() {
        ChunkTestUtils.putAndGetLockAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2);
    }
}
