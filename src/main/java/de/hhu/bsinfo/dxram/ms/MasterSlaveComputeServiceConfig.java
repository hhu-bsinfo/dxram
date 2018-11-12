package de.hhu.bsinfo.dxram.ms;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Objects;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Config for the MasterSlaveComputeService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class MasterSlaveComputeServiceConfig extends DXRAMModuleConfig {
    private static final TimeUnit PING_INTERVAL_MIN = new TimeUnit(100, TimeUnit.MS);
    private static final TimeUnit PING_INTERVAL_MAX = new TimeUnit(10, TimeUnit.SEC);

    /**
     * Compute role to assign to the current instance (master, slave or none)
     */
    @Expose
    private String m_role = ComputeRole.NONE.toString();

    /**
     * Compute group id for the current instance (ignored on none)
     */
    @Expose
    private short m_computeGroupId = 0;

    /**
     * Keep alive ping time for master to contact slaves
     */
    @Expose
    private TimeUnit m_pingInterval = new TimeUnit(1, TimeUnit.SEC);

    /**
     * Constructor
     */
    public MasterSlaveComputeServiceConfig() {
        super(MasterSlaveComputeService.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (!Objects.equals(m_role.toLowerCase(), ComputeRole.NONE_STR) && !Objects.equals(m_role.toLowerCase(),
                ComputeRole.MASTER_STR) && !Objects.equals(m_role.toLowerCase(), ComputeRole.SLAVE_STR)) {
            LOGGER.error("Invalid role string for m_role: %s", m_role);
            return false;
        }

        if (m_computeGroupId < 0) {
            LOGGER.error("Invalid m_computeGroupId %d, must be >= 0", m_computeGroupId);
            return false;
        }

        if (m_pingInterval.getMs() < PING_INTERVAL_MIN.getMs()) {
            LOGGER.error("Min m_pingInterval: %s", PING_INTERVAL_MIN.getMs());
            return false;
        }

        if (m_pingInterval.getMs() > PING_INTERVAL_MAX.getMs()) {
            LOGGER.error("Max m_pingInterval: %s", PING_INTERVAL_MAX.getMs());
            return false;
        }

        return true;
    }
}
