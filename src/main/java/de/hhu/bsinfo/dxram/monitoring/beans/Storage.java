package de.hhu.bsinfo.dxram.monitoring.beans;

import de.hhu.bsinfo.dxram.chunk.ChunkComponent;

public class Storage extends MBean implements StorageMBean {
    private final ChunkComponent m_chunk;

    public Storage(final ChunkComponent p_chunk) {
        super(Storage.class.getSimpleName());

        m_chunk = p_chunk;
    }

    @Override
    public long getAllocatedChunks() {
        return m_chunk.getMemory().stats().getLIDStoreStatus().getCurrentLIDCounter() -
                m_chunk.getMemory().stats().getLIDStoreStatus().getTotalFreeLIDs();
    }

    @Override
    public long getFreeMemory() {
        return m_chunk.getMemory().stats().getHeapStatus().getFreeSizeBytes();
    }

    @Override
    public double getFragmentation() {
        return m_chunk.getMemory().stats().getHeapStatus().getFragmentation();
    }

    @Override
    public long allocateChunk(int p_size) {
        return m_chunk.getMemory().create().create(p_size);
    }
}
