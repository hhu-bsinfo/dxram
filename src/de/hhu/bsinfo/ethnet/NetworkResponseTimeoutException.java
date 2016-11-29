/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.ethnet;

/**
 * Exception if a request was sent but the response was not delivered in time.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.10.2016
 */
public class NetworkResponseTimeoutException extends NetworkException {

    /**
     * Network Response Timeout Exception
     *
     * @param p_nodeId
     *     the NodeID
     */
    public NetworkResponseTimeoutException(final short p_nodeId) {
        super("Waiting for response from node " + NodeID.toHexString(p_nodeId) + " failed, timeout");
    }
}
