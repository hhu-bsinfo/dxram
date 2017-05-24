package de.hhu.bsinfo.dxram.mem;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Config for the MemoryManagerComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class MemoryManagerComponentConfig extends DXRAMComponentConfig {
    private static final StorageUnit KEY_VALUE_STORE_SIZE_MIN = new StorageUnit(32L, StorageUnit.MB);
    private static final StorageUnit KEY_VALUE_STORE_SIZE_MAX = new StorageUnit(128, StorageUnit.GB);

    @Expose
    private StorageUnit m_keyValueStoreSize = new StorageUnit(128L, StorageUnit.MB);

    @Expose
    private StorageUnit m_keyValueStoreMaxBlockSize = new StorageUnit(8, StorageUnit.MB);

    @Expose
    private String m_memDumpFolderOnError = "";

    /**
     * Constructor
     */
    public MemoryManagerComponentConfig() {
        super(MemoryManagerComponent.class, false, true);
    }

    /**
     * Amount of main memory to use for the key value store
     */
    public StorageUnit getKeyValueStoreSize() {
        return m_keyValueStoreSize;
    }

    /**
     * Max block size for a chunk in the key value store
     */
    public StorageUnit getKeyValueStoreMaxBlockSize() {
        return m_keyValueStoreMaxBlockSize;
    }

    /**
     * To enable mem dumps on critical errors (memory corruption), enter a path to a file to dump _ALL_ key value store memory to
     * (file size approx. key value store size)
     */
    public String getMemDumpFolderOnError() {
        return m_memDumpFolderOnError;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_keyValueStoreSize.getBytes() < KEY_VALUE_STORE_SIZE_MIN.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Min m_keyValueStoreSize: %s", KEY_VALUE_STORE_SIZE_MIN);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_keyValueStoreSize.getBytes() > KEY_VALUE_STORE_SIZE_MAX.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Max m_keyValueStoreSize: %s", KEY_VALUE_STORE_SIZE_MAX);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
