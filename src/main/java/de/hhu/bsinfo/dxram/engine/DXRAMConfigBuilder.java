package de.hhu.bsinfo.dxram.engine;

/**
 * Interface for configuration builder/modifier classes. Depending on the implementation, it can support
 * concatenating multiple builders to implement multi-stage config value overrides.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
public interface DXRAMConfigBuilder {
    /**
     * Build/modify a configuration
     *
     * @param p_config
     *         Existing (default) configuration to modify
     *
     * @return New or modified configuration
     * @throws DXRAMConfigBuilderException On any errors (e.g. reading from file, parsing etc) while config building
     */
    DXRAMConfig build(final DXRAMConfig p_config) throws DXRAMConfigBuilderException;
}
