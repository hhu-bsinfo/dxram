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

package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

/**
 * List of messages used for BFS.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.05.2016
 */
public final class BFSMessages {
    public static final byte SUBTYPE_BFS_RESULT_MESSAGE = 1;
    public static final byte SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE = 2;
    public static final byte SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE = 3;
    public static final byte SUBTYPE_BFS_TERMINATE_MESSAGE = 4;
    public static final byte SUBTYPE_PING_MESSAGE = 5;

    /**
     * Static class
     */
    private BFSMessages() {
    }
}
