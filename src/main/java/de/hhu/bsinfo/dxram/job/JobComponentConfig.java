package de.hhu.bsinfo.dxram.job;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;

/**
 * Config for the JobComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class JobComponentConfig extends ModuleConfig {
    private static final int NUM_WORKERS_MAX = 64;

    /**
     * Enable the job component which runs the work stealing worker threads to dispatch jobs
     */
    @Expose
    private boolean m_enabled = false;

    /**
     * Number of worker threads to dispatch jobs to
     */
    @Expose
    private int m_numWorkers = 1;

    /**
     * Constructor
     */
    public JobComponentConfig() {
        super(JobComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (m_numWorkers < 1) {
            LOGGER.error("Invalid value (%d) for m_numWorkers", m_numWorkers);

            return false;
        }

        if (m_numWorkers > NUM_WORKERS_MAX) {
            LOGGER.error("Max limit m_numWorkers: %d", NUM_WORKERS_MAX);

            return false;
        }

        return true;
    }
}
