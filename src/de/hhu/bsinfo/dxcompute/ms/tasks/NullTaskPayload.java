
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxcompute.ms.TaskPayload;

/**
 * Null task payload for testing.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class NullTaskPayload extends TaskPayload {

    /**
     * Constructor
     */
    public NullTaskPayload() {
        super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_NULL_TASK, NUM_REQUIRED_SLAVES_ARBITRARY);
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }
}
