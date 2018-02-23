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

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

/**
 * Different states for a peer.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 29.05.2017
 */
public enum PeerState {
    /**
     * Peer is online
     */
    ONLINE,

    /**
     * Peer is offline and was not recovered
     */
    LOST,

    /**
     * Peer is offline and its data will be recovered shortly
     */
    IN_RECOVERY,

    /**
     * Peer is offline and its data was recovered
     */
    RECOVERED,
}
