package de.hhu.bsinfo.dxram.job;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the JobWorkStealingComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class JobWorkStealingComponentConfig extends DXRAMComponentConfig {
    @Expose
    private int m_numWorkers = 1;

    /**
     * Constructor
     */
    public JobWorkStealingComponentConfig() {
        super(JobWorkStealingComponent.class, false, true);
    }

    /**
     * Number of worker threads to dispatch jobs to
     */
    public int getNumWorkers() {
        return m_numWorkers;
    }
}
