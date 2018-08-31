package de.hhu.bsinfo.dxram.chunk;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ChunkService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = ChunkService.class, supportsSuperpeer = false, supportsPeer = true)
public class ChunkServiceConfig extends DXRAMServiceConfig {
    @Expose
    private int m_removerQueueSize = 100000;

    /**
     * Size of the queue that stores the remove requests to be processed asynchronously
     */
    public int getRemoverQueueSize() {
        return m_removerQueueSize;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_removerQueueSize < 1) {
            LOGGER.error("Invalid value (%d) for m_removerQueueSize", m_removerQueueSize);

            return false;
        }

        return true;
    }

}
