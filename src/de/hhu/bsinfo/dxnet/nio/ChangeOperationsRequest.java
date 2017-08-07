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

package de.hhu.bsinfo.dxnet.nio;

/**
 * Represents a request to change the connection options
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
class ChangeOperationsRequest {

    // Attributes
    private NIOConnection m_connection;
    private int m_operations;

    // Constructors

    /**
     * Creates an instance of ChangeOperationsRequest
     *
     * @param p_connection
     *         the connection
     * @param p_operations
     *         the operations
     */
    ChangeOperationsRequest(final NIOConnection p_connection, final int p_operations) {
        m_connection = p_connection;
        m_operations = p_operations;
    }

    // Getter

    /**
     * Returns the connection
     *
     * @return the NIOConnection
     */
    public NIOConnection getConnection() {
        return m_connection;
    }

    /**
     * Returns the operation interest
     *
     * @return the operation interest
     */
    public int getOperations() {
        return m_operations;
    }

    @Override
    public boolean equals(final Object p_request) {
        return p_request instanceof ChangeOperationsRequest &&
                m_connection.getDestinationNodeID() == ((ChangeOperationsRequest) p_request).m_connection.getDestinationNodeID() &&
                m_operations == ((ChangeOperationsRequest) p_request).m_operations;
    }

    @Override
    public int hashCode() {
        int ret = 1247687943;

        ret = 37 * ret + m_connection.getDestinationNodeID();
        ret = 37 * ret + m_operations;

        return ret;
    }

    @Override
    public String toString() {
        return "[" + m_connection.getDestinationNodeID() + ", " + m_operations + ']';
    }
}
