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

package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Minimal "Hello World" example to run as DXRAM peer. This example shows how to get a service
 * and print information about the current node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 08.12.2016
 */
public final class HelloWorld extends DXRAMMain {

    /**
     * Constructor
     */
    private HelloWorld() {
        super("HelloWorld");
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        DXRAMMain main = new HelloWorld();
        main.run(p_args);
    }

    @Override
    protected int mainApplication(final String[] p_args) {
        BootService bootService = getService(BootService.class);

        System.out.println("Hello, I am a " + bootService.getNodeRole() + " and my node id is " + NodeID.toHexString(bootService.getNodeID()));

        // Put your application code running on the DXRAM node/peer here

        return 0;
    }
}
