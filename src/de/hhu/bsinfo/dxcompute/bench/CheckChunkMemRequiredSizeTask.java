package de.hhu.bsinfo.dxcompute.bench;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.unit.StorageUnit;

public class CheckChunkMemRequiredSizeTask implements Task {
    @Expose
    private StorageUnit m_minRequiredSize = new StorageUnit(1, StorageUnit.TB);
    @Expose
    private StorageUnit m_minRequiredFree = new StorageUnit(0, StorageUnit.BYTE);

    @Override
    public int execute(final TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        MemoryManagerComponent.Status status = chunkService.getStatus();

        if (status.getTotalMemory().getBytes() < m_minRequiredSize.getBytes()) {
            return -1;
        }

        if (status.getFreeMemory().getBytes() < m_minRequiredFree.getBytes()) {
            return -2;
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.exportObject(m_minRequiredSize);
        p_exporter.exportObject(m_minRequiredFree);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_minRequiredSize = new StorageUnit();
        p_importer.importObject(m_minRequiredSize);
        m_minRequiredFree = new StorageUnit();
        p_importer.importObject(m_minRequiredFree);
    }

    @Override
    public int sizeofObject() {
        return m_minRequiredSize.sizeofObject() + m_minRequiredFree.sizeofObject();
    }
}
