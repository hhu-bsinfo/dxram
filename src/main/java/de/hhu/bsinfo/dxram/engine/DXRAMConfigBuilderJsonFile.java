package de.hhu.bsinfo.dxram.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.JsonUtil;

/**
 * Build a configuration from a JSON formatted file. The config object passed to the implementation of
 * the build method is not used if an existing configuration file exists. If no configuration file exists, a new
 * one is generated on demand based on the configuration data passed to the build method.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
public class DXRAMConfigBuilderJsonFile implements DXRAMConfigBuilder {
    private static final Logger LOGGER =
            LogManager.getFormatterLogger(DXRAMConfigBuilderJsonFile.class);

    private final String m_path;
    private final boolean m_createIfNotExists;
    private final boolean m_exitAfterCreate;

    /**
     * Constructor
     *
     * @param p_path Path of the configuration file
     * @param p_createIfNotExists True to create a new default configuration if no config file exists
     */
    public DXRAMConfigBuilderJsonFile(final String p_path, final boolean p_createIfNotExists) {
        m_path = p_path;
        m_createIfNotExists = p_createIfNotExists;
        m_exitAfterCreate = false;
    }

    /**
     * Constructor
     *
     * @param p_path Path of the configuration file
     * @param p_createIfNotExists True to create a new default configuration if no config file exists
     * @param p_exitAfterCreate Exit the application after creating the default configuration
     */
    public DXRAMConfigBuilderJsonFile(final String p_path, final boolean p_createIfNotExists,
            final boolean p_exitAfterCreate) {
        m_path = p_path;
        m_createIfNotExists = p_createIfNotExists;
        m_exitAfterCreate = p_exitAfterCreate;
    }

    @Override
    public DXRAMConfig build(final DXRAMConfig p_config) throws DXRAMConfigBuilderException {
        DXRAMConfig configOut = p_config;
        Gson gson = DXRAMGsonContext.createGsonInstance();

        File configFile = new File(m_path);

        if (!configFile.exists()) {
            if (m_createIfNotExists) {
                LOGGER.debug("Config file does not exist, creating new config: %s", m_path);

                try {
                    if (!configFile.createNewFile()) {
                        throw new DXRAMConfigBuilderException("Creating new config file " + configFile + " failed");
                    }
                } catch (final IOException e) {
                    throw new DXRAMConfigBuilderException("Creating new config file " + configFile + " failed: " +
                            e.getMessage());
                }

                String jsonString = gson.toJson(p_config);

                try {
                    PrintWriter writer = new PrintWriter(configFile);
                    writer.print(jsonString);
                    writer.close();
                } catch (final FileNotFoundException e) {
                    // we can ignored this here, already checked that
                }
            } else {
                throw new DXRAMConfigBuilderException("Loading config from " + m_path + " failed, file does not exist");
            }

            if (m_exitAfterCreate) {
                LOGGER.info("Exiting after create enabled.");
                System.exit(0);
            }
        } else {
            LOGGER.debug("Loading from existing JSON config file: %s", m_path);

            try {
                JsonElement element = gson.fromJson(new String(Files.readAllBytes(Paths.get(m_path))),
                        JsonElement.class);
                configOut = gson.fromJson(element, DXRAMConfig.class);
            } catch (final Exception e) {
                throw new DXRAMConfigBuilderException("Loading JSON configuration " + m_path + " failed: " +
                        e.getMessage());
            }
        }

        return configOut;
    }
}
