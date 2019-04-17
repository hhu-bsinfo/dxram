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

import org.junit.Ignore;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * This test works only, if the LoaderComponent is registered in the SystemClassLoader,
 * this is not possible with gradle testing (gradle 4)
 */
@Ignore
@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)

        })
public class RemoteDepsTest {
    @TestInstance(runOnNodeIdx = 1)
    public void initSuperpeer(final DXRAM p_instance) throws Exception {
        LoaderService loaderService = p_instance.getService(LoaderService.class);
        loaderService.addJar(Paths.get("src/extTest/resources/dxrest.jar"));
    }

    @TestInstance(runOnNodeIdx = 2)
    public void simpleTest(final DXRAM p_instance) throws Exception {
        Thread.sleep(100);

        ApplicationService applicationService = p_instance.getService(ApplicationService.class);
        applicationService.registerApplicationClass(ExternalDepsApp.class);
        applicationService.startApplication("de.hhu.bsinfo.dxram.loader.ExternalDepsApp");
    }
}
