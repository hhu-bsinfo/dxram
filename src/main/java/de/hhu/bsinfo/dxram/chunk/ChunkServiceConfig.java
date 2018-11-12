package de.hhu.bsinfo.dxram.chunk;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;

/**
 * Config for the ChunkService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class ChunkServiceConfig extends DXRAMModuleConfig {
    /**
     * Size of the queue that stores the remove requests to be processed asynchronously
     */
    @Expose
    private int m_removerQueueSize = 100000;

    /**
     * Constructor
     */
    public ChunkServiceConfig() {
        super(ChunkService.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (m_removerQueueSize < 1) {
            LOGGER.error("Invalid value (%d) for m_removerQueueSize", m_removerQueueSize);

            return false;
        }

        return true;
    }

}
