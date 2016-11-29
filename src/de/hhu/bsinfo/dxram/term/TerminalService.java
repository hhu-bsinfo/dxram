package de.hhu.bsinfo.dxram.term;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.JNIconsole;

/**
 * Service providing an interactive terminal running on a DXRAM instance.
 * Allows access to implemented services, triggering commands, getting information
 * about current or remote DXRAM instances. The command line interface is basically a java script interpreter with
 * a few built in special commands
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
 */
public class TerminalService extends AbstractDXRAMService {

    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalService.class.getSimpleName());

    // configuration values
    @Expose
    private String m_autostartScript = "";

    // dependent components
    private AbstractBootComponent m_boot;
    private TerminalComponent m_terminal;

    private volatile boolean m_loop = true;
    private BufferedWriter m_historyFile;

    /**
     * Constructor
     */
    public TerminalService() {
        super("term");
    }

    /**
     * Run the terminal loop.
     * Only returns if terminal was exited.
     */
    public void loop() {
        byte[] arr;

        if (m_boot.getNodeRole() != NodeRole.TERMINAL) {
            System.out.println("A Terminal node must have the NodeRole \"terminal\". Aborting");
            return;
        }

        // register commands for auto completion
        JNIconsole.autocompleteCommands(m_terminal.getRegisteredCommands().keySet().toArray(new String[m_terminal.getRegisteredCommands().size()]));

        // #if LOGGER >= INFO
        LOGGER.info("Running terminal...");
        // #endif /* LOGGER >= INFO */

        System.out.println("Running on node " + NodeID.toHexString(m_boot.getNodeID()) + ", role " + m_boot.getNodeRole());
        System.out.println("Type '?' or 'help' to print the help message");

        // auto start script file
        if (!m_autostartScript.isEmpty()) {
            System.out.println("Running auto start script " + m_autostartScript);
            if (!m_terminal.getScriptContext().load(m_autostartScript)) {
                System.out.println("Running auto start script failed");
            } else {
                System.out.println("Running auto start script complete");
            }
        }

        while (m_loop) {
            arr = JNIconsole.readline('$' + NodeID.toHexString(m_boot.getNodeID()) + "> ");
            if (arr != null) {
                String command = new String(arr, 0, arr.length);

                try {
                    if (m_historyFile != null) {
                        m_historyFile.write(command + '\n');
                    }
                } catch (final IOException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Writing history file failed", e);
                    // #endif /* LOGGER >= ERROR */
                }

                evaluate(command);
            }
        }

        // #if LOGGER >= INFO
        LOGGER.info("Exiting terminal...");
        // #endif /* LOGGER >= INFO */
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_terminal = p_componentAccessor.getComponent(TerminalComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        if (m_boot.getNodeRole() == NodeRole.TERMINAL) {
            DXRAMJNIManager.loadJNIModule("JNIconsole");

            loadHistoryFromFile("dxram_term_history");
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        if (m_historyFile != null) {
            try {
                m_historyFile.close();
            } catch (final IOException ignored) {
            }
        }

        return true;
    }

    /**
     * Evaluate the text entered in the terminal.
     *
     * @param p_text
     *     Text to evaluate
     */
    private void evaluate(final String p_text) {

        // skip empty
        if (p_text.isEmpty()) {
            return;
        }

        if (p_text.startsWith("?")) {
            m_terminal.getScriptTerminalContext().help();
        } else if ("exit".equals(p_text)) {
            m_loop = false;
        } else if ("clear".equals(p_text)) {
            // ANSI escape codes (clear screen, move cursor to first row and first column)
            System.out.print("\033[H\033[2J");
            System.out.flush();
        } else {
            eveluateCommand(p_text);
        }
    }

    /**
     * Evaluate the terminal command
     *
     * @param p_text
     *     Text to evaluate as terminal command
     */
    private void eveluateCommand(final String p_text) {
        // resolve terminal cmd "macros"
        String[] tokensFunc = p_text.split("\\(");
        String[] tokensHelp = p_text.split(" ");

        // print help for cmd
        if (tokensHelp.length > 1 && "help".equals(tokensHelp[0])) {
            de.hhu.bsinfo.dxram.script.ScriptContext scriptCtx = m_terminal.getRegisteredCommands().get(tokensHelp[1]);
            if (scriptCtx != null) {
                m_terminal.getScriptContext().eval("dxterm.cmd(\"" + tokensHelp[1] + "\").help()");
            } else {
                System.out.println("Could not find help for terminal command '" + tokensHelp[1] + '\'');
            }
        } else if (tokensFunc.length > 1) {

            // resolve cmd call
            de.hhu.bsinfo.dxram.script.ScriptContext scriptCtx = m_terminal.getRegisteredCommands().get(tokensFunc[0]);
            if (scriptCtx != null) {
                // load imports
                m_terminal.getScriptContext().eval("dxterm.cmd(\"" + tokensFunc[0] + "\").imports()");

                // assemble long call
                String call = "dxterm.cmd(\"" + tokensFunc[0] + "\").exec(";

                // prepare parameters
                if (tokensFunc[1].length() > 1) {
                    call += tokensFunc[1];
                } else {
                    call += ")";
                }

                m_terminal.getScriptContext().eval(call);
            } else {
                m_terminal.getScriptContext().eval(p_text);
            }
        } else {
            // filter some generic "macros"
            if ("help".equals(p_text)) {
                m_terminal.getScriptTerminalContext().help();
            } else {
                m_terminal.getScriptContext().eval(p_text);
            }
        }
    }

    /**
     * Load terminal command history from a file.
     *
     * @param p_file
     *     File to load the history from and append new commands to.
     */
    private void loadHistoryFromFile(final String p_file) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(p_file));

            // import history if found
            String str;
            while (true) {
                try {
                    str = reader.readLine();
                } catch (final IOException ignored) {
                    break;
                }

                if (str == null) {
                    break;
                }

                JNIconsole.addToHistory(str);
            }

            reader.close();
        } catch (final FileNotFoundException e) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("No history found: %s", p_file);
            // #endif /* LOGGER >= DEBUG */
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Reading history %s failed: %s", p_file, e);
            // #endif /* LOGGER >= ERROR */
        }

        try {
            m_historyFile = new BufferedWriter(new FileWriter(p_file, true));
        } catch (final IOException e) {
            m_historyFile = null;
            // #if LOGGER >= WARN
            LOGGER.warn("Opening history %s for writing failed: %s", p_file, e);
            // #endif /* LOGGER >= WARN */
        }
    }
}
