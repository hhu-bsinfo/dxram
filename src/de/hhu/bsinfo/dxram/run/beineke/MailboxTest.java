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

package de.hhu.bsinfo.dxram.run.beineke;

import java.nio.ByteBuffer;
import java.util.Arrays;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/*
 * Start-up:
 * 1) Start at least one superpeer.
 * 2) Optional: Start peers for backup.
 * 3) Start server: With parameters "server x" whereas x is the number of messages that should be stored on server
 * 4) Start clients: No parameters
 */

/**
 * Test case for the distributed Chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 07.04.2016
 */
public final class MailboxTest {

    // Constructors

    /**
     * Creates an instance of MailboxTest
     */
    private MailboxTest() {
    }

    // Methods

    /**
     * Program entry point
     *
     * @param p_arguments
     *     The program arguments
     */
    public static void main(final String[] p_arguments) {
        if (p_arguments.length == 2 && "server".equals(p_arguments[0])) {
            new Server(Integer.parseInt(p_arguments[1])).start();
        } else {
            Client.start();
        }
    }

    // Classes

    /**
     * Represents a superpeer
     *
     * @author Florian Klein, florian.klein@hhu.de, 22.07.2013
     */
    private static class Server {

        // Attributes
        private int m_amount;

        // Constructors

        /**
         * Creates an instance of Server
         *
         * @param p_amount
         *     the amount of Chunks to create
         */
        Server(final int p_amount) {
            m_amount = p_amount;
        }

        // Methods

        /**
         * Starts the server
         */
        public void start() {
            DSByteArray anchor;
            DSByteArray[] chunks;

            // Wait a moment
            try {
                Thread.sleep(500);
            } catch (final InterruptedException ignored) {
            }

            // Initialize DXRAM
            final DXRAM dxram = new DXRAM();
            dxram.initialize("config/dxram.conf");
            final ChunkService chunkService = dxram.getService(ChunkService.class);
            final NameserviceService nameService = dxram.getService(NameserviceService.class);

            // Create anchor
            anchor = new DSByteArray(Long.BYTES * m_amount);
            chunkService.create(anchor);
            nameService.register(anchor, "anc");

            // Create Mails
            chunks = new DSByteArray[m_amount];
            for (int i = 0; i < m_amount; i++) {
                chunks[i] = new DSByteArray(1024);
                byte[] tmp = ("Mail " + i).getBytes();
                System.arraycopy(tmp, 0, chunks[i].getData(), 0, tmp.length);
            }
            chunkService.create(chunks);
            chunkService.put(chunks);

            // Set the Mailbox-Content
            ByteBuffer wrapped = ByteBuffer.wrap(anchor.getData());
            for (int i = 0; i < chunks.length; i++) {
                wrapped.putLong(i * Long.BYTES, chunks[i].getID());
            }
            chunkService.put(anchor);

            System.out.println("Server started");

            /* Migration test */
            int i = 0;
            while (i < 10) {
                // Wait a moment
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {
                }
                i++;
            }
            // try {
            // System.out.println("Migrating range(1,5) to " + 960);
            // Core.migrateRange(((long) NodeID.getLocalNodeID() << 48) + 1,
            // ((long) NodeID.getLocalNodeID() << 48) + 5,
            // (short) 960);
            // System.out.println("Migrating object(1) to " + 320);
            // Core.migrate(((long) NodeID.getLocalNodeID() << 48) + 1, (short) 320);
            // Core.execute("migrate", "" + ((long) NodeID.getLocalNodeID() << 48) + 1, "" + Core.getNodeID(), ""
            // + (short) 320);
            // System.out.println("Migrating object(2) to " + (-15999));
            // Core.migrate(((long) NodeID.getLocalNodeID() << 48) + 2, (short) (-15999));
            // System.out.println("Migrating object(3) to " + (-15615));
            // Core.migrate(((long) NodeID.getLocalNodeID() << 48) + 3, (short) (-15615));
            // } catch (final DXRAMException e1) {}

            while (true) {
                // Wait a moment
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {
                }
            }
        }
    }

    /**
     * Represents a client
     *
     * @author Florian Klein
     *         22.07.2013
     */
    private static final class Client {

        // Constructors

        /**
         * Creates an instance of Client
         */
        private Client() {
        }

        // Methods

        /**
         * Starts the client
         */
        public static void start() {
            int i = 0;
            long chunkID;
            long[] chunkIDs;
            ByteBuffer data;
            ChunkAnon anchor;
            DSByteBuffer chunk;

            // Wait a moment
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ignored) {
            }

            // Initialize DXRAM
            final DXRAM dxram = new DXRAM();
            dxram.initialize("config/dxram.json");
            final ChunkService chunkService = dxram.getService(ChunkService.class);
            final ChunkAnonService chunkAnonService = dxram.getService(ChunkAnonService.class);
            final NameserviceService nameService = dxram.getService(NameserviceService.class);

            System.out.println("Client started");

            chunkID = nameService.getChunkID("anc", -1);
            System.out.println(ChunkID.toHexString(chunkID));
            ChunkAnon[] tmp = new ChunkAnon[1];
            chunkAnonService.get(tmp, chunkID);
            anchor = tmp[0];
            System.out.println(anchor);

            chunkIDs = new long[anchor.getData().length / Long.BYTES];
            ByteBuffer wrapped = ByteBuffer.wrap(anchor.getData());
            while (wrapped.remaining() >= Long.BYTES) {
                chunkIDs[i++] = wrapped.getLong();
            }

            chunk = new DSByteBuffer(1024);
            // Get the Mailbox-Content
            while (true) {
                System.out.println("----------");

                for (long id : chunkIDs) {
                    chunk.setID(id);
                    chunkService.get(chunk);
                    System.out.println(new String(Arrays.copyOfRange(chunk.getData().array(), 0, 10)) + '\t' + ChunkID.toHexString(chunk.getID()));
                }

                // Wait a moment
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {
                }
            }
        }
    }

}
