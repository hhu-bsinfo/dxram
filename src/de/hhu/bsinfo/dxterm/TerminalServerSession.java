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

package de.hhu.bsinfo.dxterm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Remote terminal session on the server running in a separate thread
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TerminalServerSession implements Runnable {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalServerSession.class.getSimpleName());

    private final TerminalServer m_server;
    private final TerminalSession m_session;
    private final TerminalServiceAccessor m_services;

    /**
     * Constructor
     *
     * @param p_server
     *         Terminal server object
     * @param p_session
     *         Opened session with remove client
     * @param p_services
     *         Service accessor for terminal commands
     */
    public TerminalServerSession(final TerminalServer p_server, final TerminalSession p_session, final TerminalServiceAccessor p_services) {
        m_server = p_server;
        m_session = p_session;
        m_services = p_services;
    }

    @Override
    public void run() {
        TerminalLogin login = new TerminalLogin(m_session.getSessionId(), m_server.getNodeId(), m_server.getTerminalCommandList());

        if (!m_session.write(login)) {
            // #if LOGGER == ERROR
            LOGGER.error("Sending login object failed");
            // #endif /* LOGGER == ERROR */

            m_session.close();
            return;
        }

        while (true) {
            Object object = m_session.read();

            if (object != null) {
                if (object instanceof TerminalCommandString) {
                    m_server.evaluate(m_session, (TerminalCommandString) object, m_services);

                    m_session.write(new TerminalCommandDone());
                } else if (object instanceof TerminalLogout) {
                    m_session.close();
                    return;
                } else {
                    // #if LOGGER == ERROR
                    LOGGER.error("Received invalid object", object);
                    // #endif /* LOGGER == ERROR */
                }
            } else {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {

                }
            }
        }
    }
}
