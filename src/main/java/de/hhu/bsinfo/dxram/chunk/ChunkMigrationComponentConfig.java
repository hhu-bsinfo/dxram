package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the ChunkMigrationComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMComponentConfig.Settings(component = ChunkMigrationComponent.class, supportsSuperpeer = false,
        supportsPeer = true)
public class ChunkMigrationComponentConfig extends DXRAMComponentConfig {

}
