package de.hhu.bsinfo.dxram.chunk;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
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
public class ChunkServiceMulti5Test {
    @ClientInstance
    private DXRAM m_instance;

    private ChunkIDRanges m_cidRanges;

    @BeforeClass
    public void setup() {
        m_cidRanges = new ChunkIDRanges();
        ChunkTestUtils.createChunksMulti(m_instance, m_cidRanges, false, ChunkTestConstants.CHUNK_SIZE_5, 100);
        ChunkTestUtils.createChunksMulti(m_instance, m_cidRanges, true, ChunkTestConstants.CHUNK_SIZE_5, 100);
    }

    @Test
    public void putAndGet2() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 2);
    }

    @Test
    public void putAndGet4() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 4);
    }

    @Test
    public void putAndGet8() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 8);
    }

    @Test
    public void putAndGet10() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 10);
    }

    @Test
    public void putAndGet15() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 15);
    }

    @Test
    public void putAndGet20() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 20);
    }

    @Test
    public void putAndGet30() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 30);
    }

    @Test
    public void putAndGet40() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 40);
    }

    @Test
    public void putAndGet50() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 50);
    }

    @Test
    public void putAndGet100() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_5, 100);
    }
}
