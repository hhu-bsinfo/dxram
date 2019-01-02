package de.hhu.bsinfo.dxram.monitoring.beans;

import de.hhu.bsinfo.dxmem.DXMem;

public class Storage extends MBean implements StorageMBean {

    private final DXMem m_memory;

    public Storage(DXMem p_memory) {
        super(Storage.class.getSimpleName());
        m_memory = p_memory;
    }

    @Override
    public long getAllocatedChunks() {
        return m_memory.stats().getLIDStoreStatus().getCurrentLIDCounter() - m_memory.stats().getLIDStoreStatus().getTotalFreeLIDs();
    }

    @Override
    public long getFreeMemory() {
        return m_memory.stats().getHeapStatus().getFreeSizeBytes();
    }

    @Override
    public double getFragmentation() {
        return m_memory.stats().getHeapStatus().getFragmentation();
    }

    @Override
    public long allocateChunk(int p_size) {
        return m_memory.create().create(p_size);
    }
}
