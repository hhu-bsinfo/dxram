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

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class DistributeJarTest {
    @TestInstance(runOnNodeIdx = 3)
    public void register(final DXRAM p_instance) {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("src/extTest/resources/dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 0)
    public void check0(final DXRAM p_instance) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        Assert.assertEquals(4, loaderService.numberLoadedEntries());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void check1(final DXRAM p_instance) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        Assert.assertEquals(4, loaderService.numberLoadedEntries());
    }

    @TestInstance(runOnNodeIdx = 2)
    public void check2(final DXRAM p_instance) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        Assert.assertEquals(4, loaderService.numberLoadedEntries());
    }
}
