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

import java.util.List;

/**
 * Usually the concept does not allow a DXRAMService to use or even know
 * other DXRAMServices to keep things properly modularized. But there
 * might be exceptions (the less the better). One is a Job, which might execute
 * external/user code. To avoid writing wrappers and glue code to create another
 * API for the Jobs, we allow access to services inside the Jobs execution routine.
 * So far only the Jobs are using this. Do not use this anywhere else if there
 * aren't very good/similar reasons for it (refer to Jobs).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface DXRAMServiceAccessor {
    /**
     * Get a list of short names of all available services.
     *
     * @return List of (short) service names.
     */
    List<String> getServiceShortNames();

    /**
     * Get a service from DXRAM.
     *
     * @param p_class
     *         Class of the service to get. If the service has different implementations, use the common interface
     *         or abstract class to get the registered instance.
     * @param <T>
     *         Class extending DXRAMService
     * @return Reference to the service if available and enabled, null otherwise.
     */
    <T extends AbstractDXRAMService> T getService(final Class<T> p_class);

    /**
     * Get a service by its short name/identifier.
     *
     * @param p_shortName
     *         Short name of the service.
     * @return Service or null if no service exists for that name.
     */
    AbstractDXRAMService getService(final String p_shortName);

    /**
     * Check if a service is available/enabled
     *
     * @param p_class
     *         Class of the service to check
     * @param <T>
     *         Class extending DXRAMService
     * @return True if service available/enabled, false otherwise
     */
    <T extends AbstractDXRAMService> boolean isServiceAvailable(final Class<T> p_class);

    /**
     * Check if a service is available by its short name
     *
     * @param p_shortName
     *         Short name of service
     * @return True if service available/enabled, false otherwise
     */
    boolean isServiceAvailable(final String p_shortName);
}
