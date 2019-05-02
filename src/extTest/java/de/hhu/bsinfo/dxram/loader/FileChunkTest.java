/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.loader;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class FileChunkTest {
    long fileChunkId = 0;

    @TestInstance(runOnNodeIdx = 1)
    public void simpleTest(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        BootService bootService = p_instance.getService(BootService.class);

        File testFile = new File("test");

        if (!testFile.exists()) {
            try {
                testFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        short peer = bootService.getOnlinePeerNodeIDs()
                .stream()
                .findFirst().orElse(NodeID.INVALID_ID);

        FileChunk fileChunk = new FileChunk(testFile);
        chunkService.create().create(peer, fileChunk);
        chunkService.put().put(fileChunk);

        fileChunkId = fileChunk.getID();
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest2(final DXRAM p_instance) {
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        while (fileChunkId == 0) {
            Thread.yield();
        }

        FileChunk fileChunk = new FileChunk();
        Assert.assertTrue(fileChunk.getM_name().equals(""));
        fileChunk.setID(fileChunkId);
        Assert.assertTrue(chunkService.get().get(fileChunk));

        Assert.assertTrue(fileChunk.getM_name().equals("test"));
    }
}
