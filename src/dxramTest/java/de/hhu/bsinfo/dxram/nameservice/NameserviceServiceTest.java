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

package de.hhu.bsinfo.dxram.nameservice;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxram.ClientInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.data.ChunkID;

//@RunWith(DXRAMJunitRunner.class)
//@DXRAMRunnerConfiguration(expectedNodes = 3)
public class NameserviceServiceTest {

    @ClientInstance
    private DXRAM m_instance = null;

    @Test
    public void testNameService() {
        NameserviceService nameserviceService = m_instance.getService(NameserviceService.class);
        nameserviceService.register(ChunkID.getChunkID((short) 0x1234, 42), "TEST");
        long chunkId = nameserviceService.getChunkID("TEST", 1000);

        Assert.assertEquals(42, ChunkID.getLocalID(chunkId));
    }
}
