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

package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Helper methods for superpeer overlay
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 02.04.2016
 */
public final class OverlayHelper {

    // Constants
    public static final short ORDER = 10;
    private static final Logger LOGGER = LogManager.getFormatterLogger(OverlayHelper.class);

    /**
     * Hidden constructor
     */
    private OverlayHelper() {
    }

    /**
     * Verifies if a peer is in interval (p_firstSuperpeer, p_lastSuperpeer]
     *
     * @param p_peer
     *         NodeID to compare
     * @param p_firstSuperpeer
     *         first NodeID
     * @param p_lastSuperpeer
     *         last NodeID
     * @return true if peer is in range, false otherwise
     */
    public static boolean isPeerInSuperpeerRange(final short p_peer, final short p_firstSuperpeer,
            final short p_lastSuperpeer) {
        return isIDInSuperpeerRange(p_peer, p_firstSuperpeer, p_lastSuperpeer);
    }

    /**
     * Verifies if a hash id is in interval (p_firstSuperpeer, p_lastSuperpeer]
     *
     * @param p_hashID
     *         hash id to compare
     * @param p_firstSuperpeer
     *         first NodeID
     * @param p_lastSuperpeer
     *         last NodeID
     * @return true if hash is in range, false otherwise
     */
    public static boolean isHashInSuperpeerRange(final short p_hashID, final short p_firstSuperpeer,
            final short p_lastSuperpeer) {
        return isIDInSuperpeerRange(p_hashID, p_firstSuperpeer, p_lastSuperpeer);
    }

    /**
     * Verifies if a superpeer is in (p_startID, p_endID)
     *
     * @param p_nodeID
     *         superpeer to compare
     * @param p_startID
     *         NodeID of first superpeer
     * @param p_endID
     *         NodeID of last superpeer
     * @return true if superpeer is between p_startID and p_endID, false otherwise
     */
    static boolean isSuperpeerInRange(final short p_nodeID, final short p_startID, final short p_endID) {
        boolean ret = false;

        if (p_startID == p_endID || p_startID == NodeID.INVALID_ID || p_endID == NodeID.INVALID_ID) {
            // There is only one superpeer
            if (p_nodeID != p_startID && p_nodeID != p_endID) {
                ret = true;
            }
        } else if (p_startID < p_endID) {
            // Example: m = 8, start = 2, end = 6 -> true: 3, 4, 5; false: 0, 1, 2, 6, 7
            if (p_nodeID > p_startID && p_nodeID < p_endID) {
                ret = true;
            }
        } else {
            // Example: m = 8, start = 6, end = 2 -> true: 7, 0, 1; false: 2, 3, 4, 5, 6
            if (p_nodeID > p_startID || p_nodeID < p_endID) {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * Checks if the superpeer overlay is stable by comparing the current number of superpeers with the initially
     * expected
     *
     * @param p_initialNumberOfSuperpeers
     *         number of superpeers in nodes configuration
     * @param p_currentNumberOfSuperpeers
     *         number of currently available superpeers
     * @return whether the overlay is stable or not
     */
    static boolean isOverlayStable(final int p_initialNumberOfSuperpeers, final int p_currentNumberOfSuperpeers) {
        return p_initialNumberOfSuperpeers == p_currentNumberOfSuperpeers;
    }

    /**
     * Inserts the superpeer at given position in the superpeer array
     *
     * @param p_superpeer
     *         NodeID of the new superpeer
     * @param p_superpeers
     *         all superpeers
     * @lock overlay lock must be write-locked
     */
    static void insertSuperpeer(final short p_superpeer, final ArrayList<Short> p_superpeers) {
        int index;

        assert p_superpeer != NodeID.INVALID_ID;

        index = Collections.binarySearch(p_superpeers, p_superpeer);
        if (index < 0) {
            p_superpeers.add(index * -1 - 1, p_superpeer);
        }
    }

    /**
     * Removes superpeer
     *
     * @param p_superpeer
     *         NodeID of the superpeer that has to be removed
     * @param p_superpeers
     *         all superpeers
     * @return the index if p_superpeer was found and deleted, -1 otherwise
     * @lock overlay lock must be write-locked
     */
    static int removeSuperpeer(final short p_superpeer, final ArrayList<Short> p_superpeers) {
        int ret = -1;
        int index;

        index = Collections.binarySearch(p_superpeers, p_superpeer);
        if (index >= 0) {
            p_superpeers.remove(index);
            ret = index;
        }

        return ret;
    }

    /**
     * Inserts the peer at given position in the peer array
     *
     * @param p_peer
     *         NodeID of the new peer
     * @param p_peers
     *         all peers
     * @lock overlay lock must be write-locked
     */
    static void insertPeer(final short p_peer, final ArrayList<Short> p_peers) {
        int index;

        assert p_peer != NodeID.INVALID_ID;

        index = Collections.binarySearch(p_peers, p_peer);
        if (index < 0) {
            p_peers.add(index * -1 - 1, p_peer);
        }
    }

    /**
     * Determines if the given peer is in given peer list
     *
     * @param p_peer
     *         NodeID of the peer
     * @param p_peers
     *         all peers
     * @return true if peer was found, false otherwise
     * @lock overlay lock must be read-locked
     */
    static boolean containsPeer(final short p_peer, final ArrayList<Short> p_peers) {
        boolean ret = false;
        int index;

        index = Collections.binarySearch(p_peers, p_peer);
        if (index >= 0) {
            ret = true;
        }

        return ret;
    }

    /**
     * Removes peer
     *
     * @param p_peer
     *         NodeID of the peer that has to be removed
     * @param p_peers
     *         all peers
     * @return true if p_peer was found and deleted, false otherwise
     * @lock overlay lock must be write-locked
     */
    static boolean removePeer(final short p_peer, final ArrayList<Short> p_peers) {
        boolean ret = false;
        int index;

        index = Collections.binarySearch(p_peers, p_peer);
        if (index >= 0) {
            p_peers.remove(index);
            ret = true;
        }

        return ret;
    }

    /**
     * Determines the superpeers given superpeer stores backups for
     *
     * @param p_nodeID
     *         the NodeID
     * @param p_predecessor
     *         the predecessor superpeer
     * @param p_superpeers
     *         all superpeers
     * @return the superpeers p_nodeID is responsible for (got backups for)
     * @lock overlay lock must be read-locked
     */
    static short[] getResponsibleArea(final short p_nodeID, final short p_predecessor,
            final ArrayList<Short> p_superpeers) {
        short[] responsibleArea;
        short size;
        short index;

        size = (short) p_superpeers.size();
        responsibleArea = new short[2];
        if (size > 3) {
            index = (short) Collections.binarySearch(p_superpeers, p_predecessor);
            if (index >= 3) {
                index -= 3;
            } else {
                index = (short) (size - (3 - index));
            }
            responsibleArea[0] = p_superpeers.get(index);
            responsibleArea[1] = p_nodeID;
        } else {
            responsibleArea[0] = p_nodeID;
            responsibleArea[1] = p_nodeID;
        }
        return responsibleArea;
    }

    /**
     * Determines the responsible superpeer for given NodeID
     *
     * @param p_nodeID
     *         NodeID from chunk whose location is searched
     * @param p_superpeers
     *         all superpeers
     * @return the responsible superpeer for given ChunkID
     * @lock overlay lock must be read-locked
     */
    static short getResponsibleSuperpeer(final short p_nodeID, final ArrayList<Short> p_superpeers) {
        short responsibleSuperpeer = NodeID.INVALID_ID;
        int index;

        LOGGER.trace("Entering getResponsibleSuperpeer with: p_nodeID=0x%X", p_nodeID);

        if (!p_superpeers.isEmpty()) {
            index = Collections.binarySearch(p_superpeers, p_nodeID);
            if (index < 0) {
                index = index * -1 - 1;
                if (index == p_superpeers.size()) {
                    index = 0;
                }
            }
            responsibleSuperpeer = p_superpeers.get(index);
        } else {

            LOGGER.warn("Do not know any other superpeer");

        }

        LOGGER.trace("Exiting getResponsibleSuperpeer");

        return responsibleSuperpeer;
    }

    /**
     * Determines the responsible superpeers (including backups) for given NodeID
     *
     * @param p_nodeID
     *         the NodeID
     * @param p_superpeers
     *         all superpeers
     * @return the responsible superpeers for given NodeID
     * @lock overlay lock must be read-locked
     */
    static short[] getResponsibleSuperpeers(final short p_nodeID, final ArrayList<Short> p_superpeers) {
        short[] ret;
        short responsibleSuperpeer;
        short[] backupSuperpeers;
        int index;

        if (p_superpeers.isEmpty()) {
            return null;
        }

        if (p_superpeers.size() <= 4) {
            ret = new short[p_superpeers.size()];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = p_superpeers.get(i);
            }
            return ret;
        }

        index = Collections.binarySearch(p_superpeers, p_nodeID);
        if (index < 0) {
            index = index * -1 - 1;
            if (index == p_superpeers.size()) {
                index = 0;
            }
        }
        responsibleSuperpeer = p_superpeers.get(index);

        backupSuperpeers = getBackupSuperpeers(responsibleSuperpeer, p_superpeers);
        ret = new short[backupSuperpeers.length + 1];
        System.arraycopy(backupSuperpeers, 0, ret, 1, backupSuperpeers.length);
        ret[0] = responsibleSuperpeer;

        return ret;
    }

    /**
     * Determines the backup superpeers for this superpeer
     *
     * @param p_nodeID
     *         the NodeID
     * @param p_superpeers
     *         all superpeers
     * @return the three successing superpeers
     * @lock overlay lock must be read-locked
     */
    static short[] getBackupSuperpeers(final short p_nodeID, final ArrayList<Short> p_superpeers) {
        short[] superpeers;
        int size;
        int index;

        if (p_superpeers.isEmpty()) {
            superpeers = new short[] {NodeID.INVALID_ID};
        } else {
            size = Math.min(p_superpeers.size(), 3);
            superpeers = new short[size];

            index = Collections.binarySearch(p_superpeers, p_nodeID);
            if (index < 0) {
                index = index * -1 - 1;
            } else {
                index++;
            }
            for (int i = 0; i < size; i++) {
                if (index == p_superpeers.size()) {
                    superpeers[i] = p_superpeers.get(0);
                    index = 1;
                } else {
                    superpeers[i] = p_superpeers.get(index);
                    index++;
                }
            }
        }
        return superpeers;
    }

    /**
     * Determines if the given superpeer is in given superpeer list
     *
     * @param p_superpeer
     *         NodeID of the superpeer
     * @param p_superpeers
     *         all superpeers
     * @return true if superpeer was found, false otherwise
     * @lock overlay lock must be read-locked
     */
    private static boolean containsSuperpeer(final short p_superpeer, final ArrayList<Short> p_superpeers) {
        boolean ret = false;
        int index;

        index = Collections.binarySearch(p_superpeers, p_superpeer);
        if (index >= 0) {
            ret = true;
        }

        return ret;
    }

    /**
     * Verifies if an id is in interval (p_firstSuperpeer, p_lastSuperpeer]
     *
     * @param p_id
     *         id to compare (NodeID of a peer, or hashed id of an nameservice entry for instance)
     * @param p_firstSuperpeer
     *         first NodeID
     * @param p_lastSuperpeer
     *         last NodeID
     * @return true if peer is in range, false otherwise
     */
    private static boolean isIDInSuperpeerRange(final short p_id, final short p_firstSuperpeer,
            final short p_lastSuperpeer) {
        boolean ret = false;

        if (p_firstSuperpeer == p_lastSuperpeer || p_firstSuperpeer == NodeID.INVALID_ID ||
                p_lastSuperpeer == NodeID.INVALID_ID) {
            // In all three cases there is no other superpeer -> the only superpeer must be responsible
            ret = true;
        } else if (p_firstSuperpeer < p_lastSuperpeer) {
            // Example: m = 8, start = 2, end = 6 -> true: 3, 4, 5, 6; false: 0, 1, 2, 7
            if (p_id > p_firstSuperpeer && p_id <= p_lastSuperpeer) {
                ret = true;
            }
        } else {
            // Example: m = 8, start = 6, end = 2 -> true: 7, 0, 1, 2; false: 3, 4, 5, 6
            if (p_id > p_firstSuperpeer || p_id <= p_lastSuperpeer) {
                ret = true;
            }
        }

        return ret;
    }
}
