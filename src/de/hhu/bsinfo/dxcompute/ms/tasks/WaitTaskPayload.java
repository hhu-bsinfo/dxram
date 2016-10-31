
package de.hhu.bsinfo.dxcompute.ms.tasks;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxcompute.ms.TaskPayload;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Wait for specified amount of time.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class WaitTaskPayload extends TaskPayload {

    @Expose
    private int m_waitMs;

    /**
     * Constructor
     * @param p_numReqSlaves
     *            Num of slaves request to run this job
     * @param p_timeMs
     *            Amount of time to wait in ms.
     */
    public WaitTaskPayload(final short p_numReqSlaves, final int p_timeMs) {
        super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_WAIT_TASK, p_numReqSlaves);
        m_waitMs = p_timeMs;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        try {
            Thread.sleep(m_waitMs);
        } catch (final InterruptedException e) {
            return -1;
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);

        p_exporter.writeInt(m_waitMs);
    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);

        m_waitMs = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Integer.BYTES;
    }
}
