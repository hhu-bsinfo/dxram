package de.hhu.bsinfo.dxram.chunk;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxmem.core.Address;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the ChunkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class ChunkComponentConfig extends ModuleConfig {
    private static final StorageUnit KEY_VALUE_STORE_SIZE_MIN = new StorageUnit(1L, StorageUnit.MB);
    private static final StorageUnit KEY_VALUE_STORE_SIZE_MAX = new StorageUnit((long) Math.pow(2, Address.WIDTH_BITS),
            StorageUnit.BYTE);

    /**
     * Amount of main memory to use for the key value store
     */
    @Expose
    private StorageUnit m_keyValueStoreSize = new StorageUnit(128L, StorageUnit.MB);

    /**
     * To enable mem dumps on critical errors (memory corruption), enter a path to a file to dump _ALL_ key value store memory to
     * (file size approx. key value store size)
     */
    @Expose
    private String m_memDumpFolderOnError = "";

    /**
     * Disable the chunk lock mechanism which increases performance but blocks the remove
     * and resize operations. All lock operation arguments provided on operation calls are
     * ignored. DXMem cannot guarantee application data consistency on parallel writes to
     * the same chunk. Useful for read only applications or if the application handles
     * synchronization when writing to chunks.
     */
    @Expose
    private boolean m_chunkLockDisabled = false;

    /**
     * Enable/disable the chunk storage. A peer with disabled chunk storage is no longer considered a storage peer.
     */
    @Expose
    private boolean m_chunkStorageEnabled = true;

    /**
     * Constructor
     */
    public ChunkComponentConfig() {
        super(ChunkComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
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
