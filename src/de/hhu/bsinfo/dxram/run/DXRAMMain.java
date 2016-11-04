package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Base class for an entry point of a DXRAM application.
 * If DXRAM is integrated into an existing application,
 * just use the DXRAM class instead.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.02.2016
 */
public class DXRAMMain extends AbstractMain {

    private DXRAM m_dxram;
    private String m_nodeTypeName;

    /**
     * Default constructor
     */
    public DXRAMMain() {
        super("DXRAM");
        m_dxram = new DXRAM();
        m_nodeTypeName = "DXRAM";
    }

    /**
     * Constructor
     * Use this if you extended the DXRAM class and provide an instance of it to
     * run it within the DXRAMMain context
     *
     * @param p_nodeTypeName
     *     Type name for node (debugging purpose, only)
     * @param p_dxram
     *     DXRAM instance to run (just create the instance, no init)
     */
    public DXRAMMain(final String p_nodeTypeName, final DXRAM p_dxram) {
        super("DXRAM");
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

    @Override
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {

    }

    @Override
    protected int main(final ArgumentList p_arguments) {
        printBuildDateAndUser();
        System.out.println("Main entry point: " + m_nodeTypeName);

        if (!m_dxram.initialize(true)) {
            System.out.println("Initializing " + m_nodeTypeName + " failed.");
            return -1;
        }

        return mainApplication(p_arguments);
    }

    /**
     * Override this to implement your application built on top of DXRAM.
     *
     * @param p_arguments
     *     the arguments
     * @return Exit code of the application.
     */
    protected int mainApplication(final ArgumentList p_arguments) {
        BootService boot = getService(BootService.class);

        if (boot != null) {
            NodeRole role = boot.getNodeRole();

            if (role == NodeRole.TERMINAL) {
                System.out.println(">>> " + m_nodeTypeName + " Terminal started <<<");
                if (!runTerminal()) {
                    return -1;
                } else {
                    return 0;
                }
            } else {
                System.out.println(">>> " + m_nodeTypeName + " started <<<");

                while (true) {
                    // Wait
                    try {
                        Thread.sleep(100000);
                    } catch (final InterruptedException ignored) {
                    }
                }
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
