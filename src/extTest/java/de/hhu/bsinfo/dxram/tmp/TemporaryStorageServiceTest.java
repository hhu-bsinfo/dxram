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
package de.hhu.bsinfo.dxram.tmp;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 15.02.2019
 *
 */
@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class TemporaryStorageServiceTest {

	@TestInstance(runOnNodeIdx = 1)
	public void createSimple(final DXRAM p_instance) {
        TemporaryStorageService tmpStorageService = p_instance.getService(TemporaryStorageService.class);

        int id = 123;
        int size = 1;
        byte[] buffer = new byte[size];
        buffer[0] = 42;
        ChunkAnon chunk = new ChunkAnon(id, buffer);
        boolean ret = tmpStorageService.create(chunk);
        tmpStorageService.putAnon(chunk);

        Assert.assertTrue(ret);
    }
}
