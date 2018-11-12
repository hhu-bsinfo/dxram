package de.hhu.bsinfo.dxram.tmp;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the TemporaryStorageService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class TemporaryStorageServiceConfig extends DXRAMModuleConfig {
    private static final int STORAGE_MAX_NUM_ENTRIES_MAX = 100000;
    private static final StorageUnit STORAGE_MAX_SIZE_MAX = new StorageUnit(1, StorageUnit.GB);

    /**
     * Maximum number of entries allowed on the superpeer/temporary storage
     */
    @Expose
    private int m_storageMaxNumEntries = 1000;

    /**
     * Size of the superpeer/temporary storage
     */
    @Expose
    private StorageUnit m_storageMaxSize = new StorageUnit(32, StorageUnit.MB);

    /**
     * Constructor
     */
    public TemporaryStorageServiceConfig() {
        super(TemporaryStorageService.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (m_storageMaxNumEntries < 0) {
            LOGGER.error("Invalid value m_storageMaxNumEntries: %d", m_storageMaxNumEntries);
            return false;
        }

        if (m_storageMaxNumEntries > STORAGE_MAX_NUM_ENTRIES_MAX) {
            LOGGER.error("Max m_storageMaxNumEntries: %d", m_storageMaxNumEntries);
            return false;
        }

        if (m_storageMaxSize.getBytes() < 0) {
            LOGGER.error("Invalid value m_storageMaxSize: %d", m_storageMaxSize.getBytes());
            return false;
        }

        if (m_storageMaxSize.getBytes() > STORAGE_MAX_SIZE_MAX.getBytes()) {
            LOGGER.error("Max m_storageMaxSize: %s", STORAGE_MAX_SIZE_MAX);
            return false;
        }

        return true;
    }
}
