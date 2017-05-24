package de.hhu.bsinfo.dxram.sync;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the SynchronizationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class SynchronizationServiceConfig extends DXRAMServiceConfig {
    @Expose
    private int m_maxBarriersPerSuperpeer = 1000;

    /**
     * Constructor
     */
    public SynchronizationServiceConfig() {
        super(SynchronizationService.class, false, true);
    }

    /**
     * Maximum number of barriers that can be allocated on a single superpeer
     */
    public int getMaxBarriersPerSuperpeer() {
        return m_maxBarriersPerSuperpeer;
    }
}
