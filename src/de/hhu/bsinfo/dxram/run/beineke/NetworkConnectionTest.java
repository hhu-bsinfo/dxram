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

package de.hhu.bsinfo.dxram.run.beineke;

import java.util.Iterator;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

/*
 * Start-up:
 * 1) Start at least one superpeer.
 * 2) Optional: Start peers for backup.
 * 3) Start server: With parameters "server x" whereas x is the number of messages that should be stored on server
 * 4) Start clients: No parameters
 */

/**
 * Test case for connection creation.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 09.06.2016
 */
public final class NetworkConnectionTest {

    // Constructors

    /**
     * Creates an instance of NetworkConnectionTest
     */
    private NetworkConnectionTest() {
    }

    // Methods

    /**
     * Program entry point
     *
     * @param p_arguments
     *     The program arguments
     */
    public static void main(final String[] p_arguments) {
        int barrierID = BarrierID.INVALID_ID;

        // Initialize DXRAM
        final DXRAM dxram = new DXRAM();
        dxram.initialize("config/dxram.conf");
        final ChunkService chunkService = dxram.getService(ChunkService.class);
        final BootService bootService = dxram.getService(BootService.class);
        final NameserviceService nameService = dxram.getService(NameserviceService.class);
        final SynchronizationService synchronizationService = dxram.getService(SynchronizationService.class);

        // Barrier
        if (p_arguments.length == 0) {
            while (barrierID == BarrierID.INVALID_ID) {
                barrierID = (int) nameService.getChunkID("bar", -1);
            }

            synchronizationService.barrierSignOn(barrierID, -1);
        } else if (p_arguments.length == 1) {
            barrierID = synchronizationService.barrierAllocate(Integer.parseInt(p_arguments[0]));
            nameService.register(barrierID, "bar");

            synchronizationService.barrierSignOn(barrierID, -1);
        } else {
            System.out.println("Too many or negative amount of program arguments!");
        }

        // Broadcast
        Iterator<Short> iter = bootService.getOnlinePeerNodeIDs().iterator();
        while (iter.hasNext()) {
            short nodeID = iter.next();
            chunkService.createRemote(nodeID, 50);
            chunkService.createRemote(nodeID, 50);
            chunkService.createRemote(nodeID, 50);
            chunkService.createRemote(nodeID, 50);
            chunkService.createRemote(nodeID, 50);
        }

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ignored) {
            }
        }

    }

}
