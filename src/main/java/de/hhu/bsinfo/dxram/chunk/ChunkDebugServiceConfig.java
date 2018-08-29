package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ChunkDebugService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = ChunkDebugService.class, supportsSuperpeer = false, supportsPeer = true)
public class ChunkDebugServiceConfig extends DXRAMServiceConfig {

}
