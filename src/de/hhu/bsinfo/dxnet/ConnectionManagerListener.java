/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxnet;

/**
 * Interface to listen to events from the ConnectionManager
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
public interface ConnectionManagerListener {

    /**
     * Called when a new connection was created
     *
     * @param p_destination
     *         Destination node id of the connection created
     */
    void connectionCreated(final short p_destination);

    /**
     * Called when a connection was lost
     *
     * @param p_destination
     *         Destination node id of the connection lost
     */
    void connectionLost(final short p_destination);
}
