package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ChunkAsyncService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(supportsSuperpeer = false, supportsPeer = true)
public class ChunkAsyncServiceConfig extends DXRAMServiceConfig {

}
