package de.hhu.bsinfo.dxram.tmp;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Config for the TemporaryStorageService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TemporaryStorageServiceConfig extends DXRAMServiceConfig {
    private static final int STORAGE_MAX_NUM_ENTRIES_MAX = 100000;
    private static final StorageUnit STORAGE_MAX_SIZE_MAX = new StorageUnit(1, StorageUnit.GB);

    @Expose
    private int m_storageMaxNumEntries = 1000;

    @Expose
    private StorageUnit m_storageMaxSize = new StorageUnit(32, StorageUnit.MB);

    /**
     * Constructor
     */
    public TemporaryStorageServiceConfig() {
        super(TemporaryStorageService.class, false, true);
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

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_storageMaxNumEntries < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid value m_storageMaxNumEntries: %d", m_storageMaxNumEntries);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_storageMaxNumEntries > STORAGE_MAX_NUM_ENTRIES_MAX) {
            // #if LOGGER >= ERROR
            LOGGER.error("Max m_storageMaxNumEntries: %d", m_storageMaxNumEntries);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_storageMaxSize.getBytes() < 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid value m_storageMaxSize: %d", m_storageMaxSize.getBytes());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_storageMaxSize.getBytes() > STORAGE_MAX_SIZE_MAX.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Max m_storageMaxSize: %s", STORAGE_MAX_SIZE_MAX);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
