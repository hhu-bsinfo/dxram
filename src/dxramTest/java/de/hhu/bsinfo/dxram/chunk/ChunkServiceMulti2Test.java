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
public class ChunkServiceMulti2Test {
    @ClientInstance
    private DXRAM m_instance;

    private ChunkIDRanges m_cidRanges;

    @BeforeClass
    public void setup() {
        m_cidRanges = new ChunkIDRanges();
        ChunkTestUtils.createChunksMulti(m_instance, m_cidRanges, false, ChunkTestConstants.CHUNK_SIZE_2, 1000);
        ChunkTestUtils.createChunksMulti(m_instance, m_cidRanges, true, ChunkTestConstants.CHUNK_SIZE_2, 1000);
    }

    @Test
    public void putAndGet2() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 2);
    }

    @Test
    public void putAndGet4() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 4);
    }

    @Test
    public void putAndGet8() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 8);
    }

    @Test
    public void putAndGet10() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 10);
    }

    @Test
    public void putAndGet15() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 15);
    }

    @Test
    public void putAndGet20() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 20);
    }

    @Test
    public void putAndGet30() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 30);
    }

    @Test
    public void putAndGet40() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 40);
    }

    @Test
    public void putAndGet50() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 50);
    }

    @Test
    public void putAndGet100() {
        ChunkTestUtils.putAndGetMultiAll(m_instance, m_cidRanges, ChunkTestConstants.CHUNK_SIZE_2, 100);
    }
}
