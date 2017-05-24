package de.hhu.bsinfo.dxram.ms;

import java.util.Objects;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Config for the MasterSlaveComputeService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class MasterSlaveComputeServiceConfig extends AbstractDXRAMServiceConfig {
    private static final TimeUnit PING_INTERVAL_MIN = new TimeUnit(100, TimeUnit.MS);
    private static final TimeUnit PING_INTERVAL_MAX = new TimeUnit(10, TimeUnit.SEC);

    @Expose
    private String m_role = ComputeRole.NONE.toString();

    @Expose
    private short m_computeGroupId = 0;

    @Expose
    private TimeUnit m_pingInterval = new TimeUnit(1, TimeUnit.SEC);

    /**
     * Constructor
     */
    public MasterSlaveComputeServiceConfig() {
        super(MasterSlaveComputeService.class, false, true);
    }

    /**
     * Compute role to assign to the current instance (master, slave or none)
     */
    public String getRole() {
        return m_role;
    }

    /**
     * Compute group id for the current instance (ignored on none)
     */
    public short getComputeGroupId() {
        return m_computeGroupId;
    }

    /**
     * Keep alive ping time for master to contact slaves
     */
    public TimeUnit getPingInterval() {
        return m_pingInterval;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (!Objects.equals(m_role.toLowerCase(), ComputeRole.NONE_STR) && !Objects.equals(m_role.toLowerCase(), ComputeRole.MASTER_STR) &&
                !Objects.equals(m_role.toLowerCase(), ComputeRole.SLAVE_STR)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid role string for m_role: %s", m_role);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_computeGroupId < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid m_computeGroupId %d, must be >= 0", m_computeGroupId);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_pingInterval.getMs() < PING_INTERVAL_MIN.getMs()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Min m_pingInterval: %s", PING_INTERVAL_MIN.getMs());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_pingInterval.getMs() > PING_INTERVAL_MAX.getMs()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Max m_pingInterval: %s", PING_INTERVAL_MAX.getMs());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
