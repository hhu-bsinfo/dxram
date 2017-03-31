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

package de.hhu.bsinfo.dxcompute.job;

/**
 * Helper class to work with job IDs.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class JobID {
    public static final long INVALID_ID = -1;

    public static final long CREATORID_BITMASK = 0xFFFF000000000000L;
    public static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;

    public static final long MAX_LOCALID = Long.MAX_VALUE & LOCALID_BITMASK;

    /**
     * Static class.
     */
    private JobID() {
    }

    /**
     * Get the CreatorID/NodeID part of the JobID.
     * @param p_jobID
     *            JobID.
     * @return The NodeID/CreatorID part.
     */
    public static short getCreatorID(final long p_jobID) {
        assert p_jobID != INVALID_ID;

        return (short) ((p_jobID & CREATORID_BITMASK) >> 48);
    }

    /**
     * Get the LocalID part of the JobID
     * @param p_jobID
     *            the JobID
     * @return the LocalID part
     */
    public static long getLocalID(final long p_jobID) {
        assert p_jobID != INVALID_ID;

        return p_jobID & LOCALID_BITMASK;
    }

    /**
     * Create a job id.
     * @param p_node
     *            Node id part.
     * @param p_id
     *            Local job id part.
     * @return Job id.
     */
    public static long createJobID(final short p_node, final long p_id) {
        assert p_node != INVALID_ID;
        assert p_id != INVALID_ID;

        return ((long) p_node << 48) | p_id;
    }
}
