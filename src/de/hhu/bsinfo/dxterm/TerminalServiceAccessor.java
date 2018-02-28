/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxterm;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Accessor interface for terminal commands to provide access to DXRAM services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public interface TerminalServiceAccessor {
    /**
     * Get a DXRAM service
     *
     * @param p_class
     *         Class of the service to get
     * @return The service if available, null otherwise
     */
    <T extends AbstractDXRAMService> T getService(final Class<T> p_class);
}
