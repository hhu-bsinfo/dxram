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

package de.hhu.bsinfo.dxram.chunk;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMRunnerConfiguration;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMRunnerConfiguration(runTestOnNodeRole = NodeRole.PEER, runTestOnNodeIdx = 0,
        zookeeperPath = "/home/nothaas/zookeeper", nodes = {
        @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.SUPERPEER, zookeeperIP = "127.0.0.1", zookeeperPort = 2181),
        @DXRAMRunnerConfiguration.Node(nodeRole = NodeRole.PEER, zookeeperIP = "127.0.0.1", zookeeperPort = 2181)
})
public class ChunkServiceTest {
    @ClientInstance
    private DXRAM m_instance;

    @Test
    public void testChunkPut() {
        ChunkService chunkService = m_instance.getService(ChunkService.class);
        long[] chunkIds = chunkService.create(64, 2);

        Assert.assertEquals(chunkIds.length, 2);
    }
}
