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

package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Base class for an entry point of a DXRAM application.
 * If DXRAM is integrated into an existing application,
 * just use the DXRAM class instead.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.02.2016
 */
public class DXRAMMain {
    private static final String STARTUP_DONE_STR = "***!---ooo---!***";

    private DXRAM m_dxram;
    private String m_nodeTypeName;
    private volatile boolean m_triggerSoftReboot;

    /**
     * Default constructor
     */
    public DXRAMMain() {
        m_dxram = new DXRAM();
        m_nodeTypeName = "DXRAM";
    }

    /**
     * Constructor
     *
     * @param p_nodeTypeName
     *     Type name for node (debugging purpose, only)
     */
    public DXRAMMain(final String p_nodeTypeName) {
        m_dxram = new DXRAM();
        m_nodeTypeName = p_nodeTypeName;
    }

    /**
     * Constructor
     * Use this if you extended the DXRAM class and provide an instance of it to
     * run it within the DXRAMMain context. This is used for building further layers
     * on top of DXRAM (refer to DXCompute or DXGraph).
     *
     * @param p_nodeTypeName
     *     Type name for node (debugging purpose, only)
     * @param p_dxram
     *     DXRAM instance to run (just create the instance, no init)
     */
    public DXRAMMain(final String p_nodeTypeName, final DXRAM p_dxram) {
        m_dxram = p_dxram;
        m_nodeTypeName = p_nodeTypeName;
    }

    /**
     * Get the DXRAM instance.
     *
     * @return DXRAM instance.
     */
    protected DXRAM getDXRAM() {
        return m_dxram;
    }

    /**
     * Main entry point
     *
     * @param p_args
     *     Program arguments.
     */
    public static void main(final String[] p_args) {
        DXRAMMain dxram = new DXRAMMain();
        dxram.run(p_args);
    }

    /**
     * DXRAM's entry point to be called from Java's main entry point.
     *
     * @param p_args
     *     Java cmd arguments
     */
    public void run(final String[] p_args) {
        System.out.println("Main entry point: " + m_nodeTypeName);

        if (!m_dxram.initialize(true)) {
            System.out.println("Initializing " + m_nodeTypeName + " failed.");
            System.exit(-1);
        }

        System.exit(mainApplication(p_args));
    }

    /**
     * Override this to implement your application built on top of DXRAM.
     *
     * @param p_args
     *     Java cmd arguments
     * @return Exit code of the application.
     */
    protected int mainApplication(final String[] p_args) {
        BootService boot = getService(BootService.class);

        if (boot != null) {
            NodeRole role = boot.getNodeRole();

            if (role == NodeRole.TERMINAL) {
                System.out.println(">>> " + m_nodeTypeName + " Terminal started <<<");
                // used for deploy script
                System.out.println(STARTUP_DONE_STR);
                if (!runTerminal()) {
                    return -1;
                } else {
                    return 0;
                }
            } else {
                System.out.println(">>> " + m_nodeTypeName + " started <<<");
                // used for deploy script
                System.out.println(STARTUP_DONE_STR);

                while (m_dxram.update()) {
                    // run
                }

                return 0;
            }
        } else {
            System.out.println("Missing BootService, cannot run " + m_nodeTypeName);
            return -1;
        }
    }

    /**
     * Get a service from DXRAM.
     *
     * @param <T>
     *     Type of the implemented service.
     * @param p_class
     *     Class of the service to get.
     * @return DXRAM service or null if not available.
     */
    protected <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return m_dxram.getService(p_class);
    }

    /**
     * Run the built in terminal. The calling thread will be used for this.
     *
     * @return True if execution was successful and finished, false if starting the terminal failed.
     */
    private boolean runTerminal() {
        TerminalService term = getService(TerminalService.class);
        if (term == null) {
            System.out.println("ERROR: Cannot run terminal, missing service.");
            return false;
        }

        term.loop();
        return true;
    }
}
