package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the ChunkMigrationComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkMigrationComponentConfig extends DXRAMComponentConfig {
    /**
     * Constructor
     */
    public ChunkMigrationComponentConfig() {
        super(ChunkMigrationComponent.class, false, true);
    }
}
