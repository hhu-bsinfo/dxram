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

package de.hhu.bsinfo.dxram.job.messages;

/**
 * Different message types used by the job package.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class JobMessages {
    public static final byte SUBTYPE_PUSH_JOB_QUEUE_MESSAGE = 1;
    public static final byte SUBTYPE_STATUS_REQUEST = 2;
    public static final byte SUBTYPE_STATUS_RESPONSE = 3;
    public static final byte SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE = 4;

    /**
     * Static class
     */
    private JobMessages() {
    }
}
