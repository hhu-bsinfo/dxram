package de.hhu.bsinfo.dxram.lookup;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Config for the LookupComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMComponentConfig.Settings(supportsSuperpeer = true, supportsPeer = true)
public class LookupComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private boolean m_cachesEnabled = true;

    @Expose
    private long m_maxCacheEntries = 1000L;

    @Expose
    private TimeUnit m_cacheTtl = new TimeUnit(1, TimeUnit.SEC);

    @Expose
    private TimeUnit m_stabilizationBreakTime = new TimeUnit(1, TimeUnit.SEC);

    /**
     * Set to enable client-side lookup caching.
     */
    public boolean cachesEnabled() {
        return m_cachesEnabled;
    }

    /**
     * Maximum number of entries in cache tree. Currently unused!
     */
    public long getMaxCacheEntries() {
        return m_maxCacheEntries;
    }

    /**
     * Time to live for btree nodes in cache tree.
     */
    public TimeUnit getCacheTtl() {
        return m_cacheTtl;
    }

    /**
     * The break time between superpeer stabilization routines (such as pinging all peers and check neighbors).
     */
    public long getStabilizationBreakTime() {
        return m_stabilizationBreakTime.getMs();
    }

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
