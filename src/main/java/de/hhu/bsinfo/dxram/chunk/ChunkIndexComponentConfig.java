package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the ChunkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */

@DXRAMComponentConfig.Settings(component = ChunkIndexComponent.class, supportsSuperpeer = false, supportsPeer = true)
public class ChunkIndexComponentConfig extends DXRAMComponentConfig {

}
