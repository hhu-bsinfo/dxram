package de.hhu.bsinfo.dxram.sync;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;

/**
 * Config for the SynchronizationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class SynchronizationServiceConfig extends AbstractDXRAMServiceConfig {
    private static final int MAX_BARRIERS_PER_SUPERPEER_MAX = 100000;

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

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_maxBarriersPerSuperpeer < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid value m_maxBarriersPerSuperpeer: %d", m_maxBarriersPerSuperpeer);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_maxBarriersPerSuperpeer > MAX_BARRIERS_PER_SUPERPEER_MAX) {
            // #if LOGGER >= ERROR
            LOGGER.error("Max m_maxBarriersPerSuperpeer: %d", MAX_BARRIERS_PER_SUPERPEER_MAX);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
