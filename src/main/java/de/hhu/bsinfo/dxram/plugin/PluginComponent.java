/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.plugin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxutils.PluginManager;

/**
 * Manage loadable jar files as plugins which are used by the application, job and master slave sub-systems
 * to load external user code.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 18.02.2019
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.PLUGIN,
        priorityShutdown = DXRAMComponentOrder.Shutdown.PLUGIN)
public class PluginComponent extends AbstractDXRAMComponent<PluginComponentConfig> {
    private PluginManager m_pluginManager;

    /**
     * Get a class by its fully qualified name from the class loader that loaded the plugin jar files.
     *
     * @param p_fullyQualifiedName
     *         Fully qualified name of class.
     * @return Class denoted by the fully qualified name.
     * @throws ClassNotFoundException
     *         If no class with the specified name was found.
     */
    public Class getClassByName(final String p_fullyQualifiedName) throws ClassNotFoundException {
        return m_pluginManager.getClassByName(p_fullyQualifiedName);
    }

    /**
     * Get a list of all sub-classes of the specified class.
     *
     * @param p_class
     *         Class to get sub-classes of.
     * @param <T>
     *         Type of the class specified as parameter.
     * @return List of sub-classes.
     */
    public <T> List<Class<? extends T>> getAllSubClasses(final Class p_class) {
        return m_pluginManager.getAllSubClasses(p_class);
    }

    public <T> List<Class<? extends T>> getAllSubClasses(final Class p_class, final String p_archiveName) {
        return m_pluginManager.getAllSubClasses(p_class, p_archiveName);
    }



    public void add(final Path p_pluginPath) {
        m_pluginManager.add(p_pluginPath);
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        PluginComponentConfig config = getConfig();

        try {
            Files.createDirectories(Paths.get(config.getPluginsPath()));
        } catch (IOException e) {
            LOGGER.warn("Couldn't create plugins directory");
        }

        while (true) {
            try {
                m_pluginManager = new PluginManager(config.getPluginsPath());
                break;
            } catch (final FileNotFoundException e) {
                File file = new File(config.getPluginsPath());

                if (!file.exists()) {
                    if (!file.mkdir()) {
                        LOGGER.error("Creating directory '%s' for plugins failed", file.getAbsolutePath());
                        return false;
                    }
                } else {
                    LOGGER.error("Path '%s' does not point to a plugin directory", file.getAbsolutePath());
                    return false;
                }

                // if directory is created, try to create plugin manager again
            }
        }

        List<String> plugins = m_pluginManager.getListPlugins();

        if (!plugins.isEmpty()) {
            LOGGER.info("Loaded %d jar plugin(s) from directory '%s'", plugins.size(), config.getPluginsPath());
        }

        if (!plugins.isEmpty()) {
            StringBuilder strBuilder = new StringBuilder();

            for (String plugin : plugins) {
                strBuilder.append(plugin);
                strBuilder.append(", ");
            }

            LOGGER.debug("Loaded plugins: %s", strBuilder.toString());
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_pluginManager = null;

        return true;
    }
}
