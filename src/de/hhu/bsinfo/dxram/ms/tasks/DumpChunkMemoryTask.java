package de.hhu.bsinfo.dxram.ms.tasks;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.chunk.ChunkDebugService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Task to dump the chunk memory to a file
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.02.2017
 */
public class DumpChunkMemoryTask implements Task {

    @Expose
    private String m_fileName = "memdump.soh";

    /**
     * Constructor
     */
    public DumpChunkMemoryTask() {

    }

    @Override
    public int execute(final TaskContext p_ctx) {
        p_ctx.getDXRAMServiceAccessor().getService(ChunkDebugService.class).dumpChunkMemory(m_fileName);

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_fileName);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_fileName = p_importer.readString(m_fileName);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_fileName);
    }
}
