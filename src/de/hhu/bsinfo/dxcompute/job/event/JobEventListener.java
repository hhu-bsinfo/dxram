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

package de.hhu.bsinfo.dxcompute.job.event;

/**
 * Listener interface to receive events triggered by the job system
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface JobEventListener {
    /**
     * Let the listener provide a bit mask with events it wants to listen to.
     * @return Bit mask indicating events to listen to.
     */
    byte getJobEventBitMask();

    /**
     * Callback if an event is triggered that the listener wants to receive.
     * @param p_eventId
     *            Id of the event triggered.
     * @param p_jobId
     *            Id of the job that triggered the event.
     * @param p_sourceNodeId
     *            The source node id that triggered the event (i.e. can be remote as well)
     */
    void jobEventTriggered(final byte p_eventId, final long p_jobId, final short p_sourceNodeId);
}
