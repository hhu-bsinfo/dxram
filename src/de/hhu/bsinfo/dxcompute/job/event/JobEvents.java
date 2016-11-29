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

package de.hhu.bsinfo.dxcompute.job.event;

/**
 * List of available job events.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class JobEvents {
    public static final byte MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID = 1 << 0;
    public static final byte MS_JOB_STARTED_EXECUTION_EVENT_ID = 1 << 1;
    public static final byte MS_JOB_FINISHED_EXECUTION_EVENT_ID = 1 << 2;

    /**
     * Static class
     */
    private JobEvents() {
    }

}
