package de.hhu.bsinfo.dxram.chunk;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the ChunkMigrationComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = ChunkMigrationComponent.class, supportsSuperpeer = false,
        supportsPeer = true)
public class ChunkMigrationComponentConfig extends DXRAMComponentConfig {

}
