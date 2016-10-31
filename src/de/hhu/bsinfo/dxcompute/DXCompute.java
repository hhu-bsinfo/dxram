
package de.hhu.bsinfo.dxcompute;

import de.hhu.bsinfo.dxcompute.job.JobService;
import de.hhu.bsinfo.dxcompute.job.JobWorkStealingComponent;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;

/**
 * Main class/entry point for any application to work with DXCompute and its services.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class DXCompute extends DXRAM {

    /**
     * Constructor
     */
    public DXCompute() {
        super();
    }

    @Override
    protected void registerComponents(final DXRAMEngine p_engine) {
        super.registerComponents(p_engine);

        p_engine.registerComponent(JobWorkStealingComponent.class);
    }

    @Override
    protected void registerServices(final DXRAMEngine p_engine) {
        super.registerServices(p_engine);

        p_engine.registerService(JobService.class);
        p_engine.registerService(MasterSlaveComputeService.class);
    }
}
