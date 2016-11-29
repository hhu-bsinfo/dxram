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

package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Minimal remote ChunkService test.
 * Run this as a peer with one superpeer and another peer
 * to receive the remote calls of the ChunkService.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public final class SimpleRemoteChunkServiceTest extends AbstractMain {
    private static final Argument ARG_REMOTE_PEER_ID = new Argument("remotePeerID", "-15999", true, "NodeID of the remote peer to create chunks on");

    private DXRAM m_dxram;
    private ChunkService m_chunkService;
    private NameserviceService m_nameserviceService;

    /**
     * Constructor
     */
    private SimpleRemoteChunkServiceTest() {
        super("Test creating chunks on a remote peer");

        m_dxram = new DXRAM();
        m_dxram.initialize("config/dxram.conf");
        m_chunkService = m_dxram.getService(ChunkService.class);
        m_nameserviceService = m_dxram.getService(NameserviceService.class);
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *         Main arguments.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new SimpleRemoteChunkServiceTest();
        main.run(p_args);
    }

    @Override protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
        p_arguments.setArgument(ARG_REMOTE_PEER_ID);
    }

    @Override protected int main(final ArgumentList p_arguments) {
        final short remotePeerID = (short) (p_arguments.getArgument(ARG_REMOTE_PEER_ID).getValue(Short.class) & 0xFFFF);

        int[] sizes = new int[] {155, 543, 99, 65, 233};
        System.out.println("Creating remote chunks...");
        long[] chunkIDs = m_chunkService.createRemote(remotePeerID, sizes);
        if (chunkIDs == null) {
            System.out.println("Creating remote chunks failed.");
            return -1;
        }
        Chunk[] chunks = new Chunk[chunkIDs.length];
        Chunk[] chunksCopy = new Chunk[chunkIDs.length];
        for (int i = 0; i < chunkIDs.length; i++) {
            chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
            chunksCopy[i] = new Chunk(chunkIDs[i], sizes[i]);
        }

        System.out.println("Remote chunks created: ");
        for (int i = 0; i < chunkIDs.length; i++) {
            System.out.println(ChunkID.toHexString(chunkIDs[i]));
        }

        System.out.println("Setting chunk payload...");
        for (Chunk chunk : chunks) {
            m_nameserviceService.register(chunk, "C" + ChunkID.getLocalID(chunk.getID()));
            System.out.println(ChunkID.toHexString(chunk.getID()) + ": " + ChunkID.toHexString(chunk.getID()));
            chunk.getData().putLong(chunk.getID());
        }

        System.out.println("Putting chunks...");
        int ret = m_chunkService.put(chunks);
        System.out.println("Putting chunks results: " + ret);

        System.out.println("Getting chunks...");
        for (int i = 0; i < chunkIDs.length; i++) {
            long chunkid = m_nameserviceService.getChunkID("C" + (i + 1), -1);
            Chunk chunk = new Chunk(chunkid, sizes[i]);
            if (m_chunkService.get(chunk) != 1) {
                System.out.println("Getting chunk failed.");
                return -1;
            }
            System.out.println(ChunkID.toHexString(chunk.getData().getLong()));
        }

        System.out.println("Removing chunks...");
        int removeCount = m_chunkService.remove(chunks);
        System.out.println("Removed chunks: " + removeCount);
        return 0;
    }
}
