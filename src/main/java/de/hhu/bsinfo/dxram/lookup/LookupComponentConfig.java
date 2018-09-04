package de.hhu.bsinfo.dxram.lookup;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Config for the LookupComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = LookupComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class LookupComponentConfig extends DXRAMComponentConfig {
    /**
     * Set to enable client-side lookup caching.
     */
    @Expose
    private boolean m_cachesEnabled = true;

    /**
     * Maximum number of entries in cache tree. Currently unused!
     */
    @Expose
    private long m_maxCacheEntries = 1000L;

    /**
     * Time to live for btree nodes in cache tree.
     */
    @Expose
    private TimeUnit m_cacheTtl = new TimeUnit(1, TimeUnit.SEC);

    /**
     * The break time between superpeer stabilization routines (such as pinging all peers and check neighbors).
     */
    @Expose
    private TimeUnit m_stabilizationBreakTime = new TimeUnit(1, TimeUnit.SEC);

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_cacheTtl.getMs() < 1000L) {
            LOGGER.warn("A high effort is needed to satisfy TTL!");
        }

        if (m_stabilizationBreakTime.getMs() < 100L) {
            LOGGER.warn("Low break time might cause high CPU load!");
        } else if (m_stabilizationBreakTime.getMs() > 1000L) {
            LOGGER.warn("Failure detection might be impeded by high break time!");
        }

        return true;
    }
}
