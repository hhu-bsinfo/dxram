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

package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Base class for an entry point of a DXRAM application.
 * If DXRAM is integrated into an existing application,
 * just use the DXRAM class instead.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.02.2016
 */
public final class DXRAMMain {
    private DXRAM m_dxram;

    /**
     * Default constructor
     */
    public DXRAMMain() {
        m_dxram = new DXRAM();
    }

    /**
     * Main entry point
     *
     * @param p_args
     *         Program arguments.
     */
    public static void main(final String[] p_args) {
        DXRAMMain dxram = new DXRAMMain();
        dxram.run(p_args);
    }

    /**
     * DXRAM's entry point to be called from Java's main entry point.
     *
     * @param p_args
     *         Java tcmd arguments
     */
    public void run(final String[] p_args) {
        System.out.println("Starting DXRAM, version " + m_dxram.getVersion());

        if (!m_dxram.initialize(true)) {
            System.out.println("Initializing DXRAM failed.");
            System.exit(-1);
        }

        System.exit(mainApplication(p_args));
    }

    /**
     * Override this to implement your application built on top of DXRAM.
     *
     * @param p_args
     *         Java tcmd arguments
     * @return Exit code of the application.
     */
    protected int mainApplication(final String[] p_args) {
        BootService boot = m_dxram.getService(BootService.class);

        if (boot != null) {
            NodeRole role = boot.getNodeRole();

            if (role == NodeRole.TERMINAL) {
                System.out.println(">>> DXRAM terminal started <<<");
                if (!runTerminal()) {
                    return -1;
                } else {
                    return 0;
                }
            } else {
                System.out.println(">>> DXRAM started <<<");

                while (m_dxram.update()) {
                    // run
                }

                return 0;
            }
        } else {
            System.out.println("Missing BootService, cannot run DXRAM");
            return -1;
        }
    }

    /**
     * Run the built in terminal. The calling thread will be used for this.
     *
     * @return True if execution was successful and finished, false if starting the terminal failed.
     */
    private boolean runTerminal() {
        TerminalService term = m_dxram.getService(TerminalService.class);
        if (term == null) {
            System.out.println("ERROR: Cannot run terminal, missing service.");
            return false;
        }

        term.loop();
        return true;
    }
}
