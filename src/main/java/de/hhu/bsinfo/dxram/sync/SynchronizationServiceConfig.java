package de.hhu.bsinfo.dxram.sync;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the SynchronizationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class SynchronizationServiceConfig extends DXRAMServiceConfig {
    private static final int MAX_BARRIERS_PER_SUPERPEER_MAX = 100000;

    @Expose
    private int m_maxBarriersPerSuperpeer = 1000;

    /**
     * Maximum number of barriers that can be allocated on a single superpeer
     */
    public int getMaxBarriersPerSuperpeer() {
        return m_maxBarriersPerSuperpeer;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_maxBarriersPerSuperpeer < 0) {
            LOGGER.error("Invalid value m_maxBarriersPerSuperpeer: %d", m_maxBarriersPerSuperpeer);
            return false;
        }

        if (m_maxBarriersPerSuperpeer > MAX_BARRIERS_PER_SUPERPEER_MAX) {
            LOGGER.error("Max m_maxBarriersPerSuperpeer: %d", MAX_BARRIERS_PER_SUPERPEER_MAX);
            return false;
        }

        return true;
    }
}
