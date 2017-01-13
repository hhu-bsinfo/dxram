package de.hhu.bsinfo.dxcompute.bench;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class RebootNodeTask implements Task {

    @Override
    public int execute(final TaskContext p_ctx) {
        BootService boot = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        boot.rebootThisNode();

        // TODO this doesn't work properly because there seem to be active threads after engine.shutdown

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {

    }

    @Override
    public void exportObject(final Exporter p_exporter) {

    }

    @Override
    public void importObject(final Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
