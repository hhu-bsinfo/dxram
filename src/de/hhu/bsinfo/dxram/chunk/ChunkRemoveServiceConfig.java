package de.hhu.bsinfo.dxram.chunk;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ChunkRemoveService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkRemoveServiceConfig extends DXRAMServiceConfig {
    @Expose
    private int m_removerQueueSize = 100000;

    /**
     * Constructor
     */
    public ChunkRemoveServiceConfig() {
        super(ChunkRemoveService.class, false, true);
    }

    /**
     * Size of the queue that stores the remove requests to be processed asynchronously
     */
    public int getRemoverQueueSize() {
        return m_removerQueueSize;
    }
}
