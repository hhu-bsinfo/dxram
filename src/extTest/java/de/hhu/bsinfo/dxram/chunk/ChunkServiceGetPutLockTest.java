package de.hhu.bsinfo.dxram.chunk;

import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.BeforeTestInstance;
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
public class ChunkServiceGetPutLockTest {
    private ChunkIDRanges m_cidRanges;

    @BeforeTestInstance(runOnNodeIdx = 2)
    public void setup(final DXRAM p_instance) {
        m_cidRanges = new ChunkIDRanges();
        ChunkTestUtils.createChunks(p_instance, m_cidRanges, false, ChunkTestConstants.CHUNK_SIZE_2, 1000);
        ChunkTestUtils.createChunks(p_instance, m_cidRanges, true, ChunkTestConstants.CHUNK_SIZE_2, 1000);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void test(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetLockAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2);
    }
}
