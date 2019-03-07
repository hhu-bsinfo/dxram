package de.hhu.bsinfo.dxram.engine;

import java.util.Collections;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.JsonUtil;

/**
 * Reads configuration parameters from JVM parameters. Can be used to override settings loaded from a file.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
public class DXRAMConfigBuilderJVMArgs implements DXRAMConfigBuilder {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMConfigBuilderJVMArgs.class);

    @Override
    public DXRAMConfig build(final DXRAMConfig p_config) throws DXRAMConfigBuilderException {
        DXRAMConfig configOut;

        LOGGER.debug("Overriding config with values from JVM args...");

        Gson gson = DXRAMGsonContext.createGsonInstance();

        JsonElement element = gson.toJsonTree(p_config);

        if (element == null) {
            throw new DXRAMConfigBuilderException("Could not create JSON tree from existing config instance");
        }

        JsonUtil.override(element, System.getProperties(), "dxram.", Collections.singletonList("dxram.config"));

        configOut = gson.fromJson(element, DXRAMConfig.class);

        if (configOut == null) {
            throw new DXRAMConfigBuilderException("Creating config instance from JSON tree failed");
        }

        return configOut;
    }
}
