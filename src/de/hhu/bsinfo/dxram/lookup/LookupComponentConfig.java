package de.hhu.bsinfo.dxram.lookup;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.utils.unit.StorageUnit;
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
    private int m_nameserviceCacheEntries = 1000000;

    @Expose
    private TimeUnit m_cacheTtl = new TimeUnit(1, TimeUnit.SEC);

    @Expose
    private int m_pingInterval = 1;

    @Expose
    private int m_maxBarriersPerSuperpeer = 1000;

    @Expose
    private int m_storageMaxNumEntries = 1000;

    @Expose
    private StorageUnit m_storageMaxSize = new StorageUnit(32, StorageUnit.MB);

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
    public int getNameserviceCacheEntries() {
        return m_nameserviceCacheEntries;
    }

    // TODO kevin: doc
    public TimeUnit getCacheTtl() {
        return m_cacheTtl;
    }

    // TODO kevin: doc
    public int getPingInterval() {
        return m_pingInterval;
    }

    /**
     * Maximum number of barriers that can be allocated on a single superpeer
     */
    public int getMaxBarriersPerSuperpeer() {
        return m_maxBarriersPerSuperpeer;
    }

    /**
     * Maximum number of entries allowed on the superpeer/temporary storage
     */
    public int getStorageMaxNumEntries() {
        return m_storageMaxNumEntries;
    }

    /**
     * Size of the superpeer/temporary storage
     */
    public StorageUnit getStorageMaxSize() {
        return m_storageMaxSize;
    }
}
