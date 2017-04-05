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

package de.hhu.bsinfo.dxgraph.run;

import de.hhu.bsinfo.dxgraph.DXGraph;
import de.hhu.bsinfo.dxram.run.DXRAMMain;

/**
 * Base class for an entry point of a DXGraph application.
 * If DXGraph is integrated into an existing application,
 * use the DXGraph class instead.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.09.2016
 */
public class DXGraphMain extends DXRAMMain {

    /**
     * Default constructor
     */
    public DXGraphMain() {
        super("DXGraph", new DXGraph());
    }

    /**
     * Constructor
     * Use this if you extended the DXGraph class and provide an instance of it to
     * run it within the DXRAMMain context
     *
     * @param p_applicationName
     *     New application name for this DXGraph instance
     * @param p_dxgraph
     *     DXCompute instance to run (just create the instance, no init)
     */
    public DXGraphMain(final String p_applicationName, final DXGraph p_dxgraph) {
        super(p_applicationName, p_dxgraph);
    }

    /**
     * Main entry point
     *
     * @param p_args
     *     Program arguments.
     */
    public static void main(final String[] p_args) {
        DXGraphMain dxgraph = new DXGraphMain();
        dxgraph.run(p_args);
    }
}
