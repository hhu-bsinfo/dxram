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

package de.hhu.bsinfo.dxram.engine;

/**
 * Base class for all services in DXRAM. All services in DXRAM form the API for the user.
 * Furthermore, different services allow splitting functionality that can be turned on/off
 * for different applications and higher flexibility. Services use components to implement
 * their functionality. A service is not allowed to depend on another service and services
 * can not interact with each other.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 25.01.2016
 */
public abstract class AbstractDXRAMService<T> extends AbstractDXRAMModule<T> {
    /**
     * Constructor
     */
    protected AbstractDXRAMService() {
        super();
    }

    @Override
    protected boolean moduleInit(final DXRAMEngine p_engine) {
        boolean ret;

        resolveComponentDependencies(p_engine);

        try {
            ret = startService(p_engine.getConfig());
        } catch (final Exception e) {
            LOGGER.error("Starting service failed", e);

            return false;
        }

        return ret;
    }

    @Override
    protected boolean moduleShutdown() {
        return shutdownService();
    }

    /**
     * Called before the service is initialized. Get all the components your service depends on.
     *
     * @param p_moduleAccessor
     *         Component accessor that provides access to the components
     */
    protected abstract void resolveComponentDependencies(final ModuleAccessor p_moduleAccessor);

    /**
     * Called when the service is initialized. Setup data structures, read settings etc.
     *
     * @param p_config
     *         Config instance provided by the engine.
     * @return True if initialing was successful, false otherwise.
     */
    protected abstract boolean startService(final DXRAMConfig p_config);

    /**
     * Called when the service gets shut down. Cleanup your service in here.
     *
     * @return True if shutdown was successful, false otherwise.
     */
    protected abstract boolean shutdownService();
}
