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
public class ChunkServiceMultiTest {
    private ChunkIDRanges m_cidRanges;

    @BeforeTestInstance(runOnNodeIdx = 2)
    public void setup(final DXRAM p_instance) {
        m_cidRanges = new ChunkIDRanges();
        ChunkTestUtils.createChunksMulti(p_instance, m_cidRanges, false, ChunkTestConstants.CHUNK_SIZE_1, 1000);
        ChunkTestUtils.createChunksMulti(p_instance, m_cidRanges, true, ChunkTestConstants.CHUNK_SIZE_1, 1000);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet2(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 2);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet4(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 4);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet8(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 8);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet10(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 10);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet15(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 15);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet20(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 20);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet30(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 30);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet40(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 40);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet50(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 50);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void putAndGet100(final DXRAM p_instance) {
        ChunkTestUtils.putAndGetMultiAll(p_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_1, 100);
    }
}
