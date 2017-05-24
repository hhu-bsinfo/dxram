package de.hhu.bsinfo.dxram.lookup;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Config for the LookupComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class LookupComponentConfig extends DXRAMComponentConfig {
    @Expose
    private boolean m_cachesEnabled = true;

    @Expose
    private long m_maxCacheEntries = 1000L;

    @Expose
    private TimeUnit m_cacheTtl = new TimeUnit(1, TimeUnit.SEC);

    @Expose
    private int m_pingInterval = 1;

    /**
     * Constructor
     */
    public LookupComponentConfig() {
        super(LookupComponent.class, true, true);
    }

    // TODO kevin: doc
    public boolean cachesEnabled() {
        return m_cachesEnabled;
    }

    // TODO kevin: doc
    public long getMaxCacheEntries() {
        return m_maxCacheEntries;
    }

    // TODO kevin: doc
    public TimeUnit getCacheTtl() {
        return m_cacheTtl;
    }

    // TODO kevin: doc
    public int getPingInterval() {
        return m_pingInterval;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        // TODO kevin
        return true;
    }
}
