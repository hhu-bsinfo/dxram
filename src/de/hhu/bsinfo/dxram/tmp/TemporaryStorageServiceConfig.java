package de.hhu.bsinfo.dxram.tmp;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Config for the TemporaryStorageService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TemporaryStorageServiceConfig extends DXRAMServiceConfig {
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
}
