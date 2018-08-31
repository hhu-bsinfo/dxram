package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceConfig;

/**
 * Config for the ChunkLocalService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@DXRAMServiceConfig.Settings(service = ChunkLocalService.class, supportsSuperpeer = false, supportsPeer = true)
public class ChunkLocalServiceConfig extends DXRAMServiceConfig {

}
