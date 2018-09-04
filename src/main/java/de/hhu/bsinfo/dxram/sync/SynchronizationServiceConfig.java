package de.hhu.bsinfo.dxram.sync;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the SynchronizationService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMServiceConfig.Settings(service = SynchronizationService.class, supportsSuperpeer = false, supportsPeer = true)
public class SynchronizationServiceConfig extends DXRAMServiceConfig {
    private static final int MAX_BARRIERS_PER_SUPERPEER_MAX = 100000;

    /**
     * Maximum number of barriers that can be allocated on a single superpeer
     */
    @Expose
    private int m_maxBarriersPerSuperpeer = 1000;

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
