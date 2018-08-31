package de.hhu.bsinfo.dxram.chunk;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxmem.core.Address;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the ChunkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = ChunkComponent.class, supportsSuperpeer = false, supportsPeer = true)
public class ChunkComponentConfig extends DXRAMComponentConfig {
    private static final StorageUnit KEY_VALUE_STORE_SIZE_MIN = new StorageUnit(1L, StorageUnit.MB);
    private static final StorageUnit KEY_VALUE_STORE_SIZE_MAX = new StorageUnit((long) Math.pow(2, Address.WIDTH_BITS),
            StorageUnit.BYTE);

    @Expose
    private StorageUnit m_keyValueStoreSize = new StorageUnit(128L, StorageUnit.MB);

    @Expose
    private String m_memDumpFolderOnError = "";

    /**
     * Amount of main memory to use for the key value store
     */
    public StorageUnit getKeyValueStoreSize() {
        return m_keyValueStoreSize;
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
            LOGGER.error("Min m_keyValueStoreSize: %s", KEY_VALUE_STORE_SIZE_MIN);

            return false;
        }

        if (m_keyValueStoreSize.getBytes() > KEY_VALUE_STORE_SIZE_MAX.getBytes()) {
            LOGGER.error("Max m_keyValueStoreSize: %s", KEY_VALUE_STORE_SIZE_MAX);

            return false;
        }

        return true;
    }
}
