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

package de.hhu.bsinfo.dxram.ms;

/**
 * Master node entry
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class MasterNodeEntry {
    private short m_nodeId;
    private short m_cgid;

    /**
     * Constructor
     *
     * @param p_nodeId
     *         Node id of the master node
     * @param p_cgid
     *         Id of the compute group the master is leading
     */
    public MasterNodeEntry(final short p_nodeId, final short p_cgid) {
        m_nodeId = p_nodeId;
        m_cgid = p_cgid;
    }

    /**
     * Get the node id of the master
     *
     * @return Node id
     */
    public short getNodeId() {
        return m_nodeId;
    }

    /**
     * Get the id of the compute group the master is leading
     *
     * @return Compute group id
     */
    public short getComputeGroupId() {
        return m_cgid;
    }
}
