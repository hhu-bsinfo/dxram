package de.hhu.bsinfo.dxram.chunk;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the ChunkRemoveService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@AbstractDXRAMServiceConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class ChunkRemoveServiceConfig extends AbstractDXRAMServiceConfig {
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
