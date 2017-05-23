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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TerminalSession {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalSession.class.getSimpleName());

    private final byte m_sessionId;
    private final Socket m_socket;
    private final Listener m_listener;
    private ObjectInputStream m_in;
    private ObjectOutputStream m_out;

    public TerminalSession(final byte p_sessionId, final Socket p_socket, final Listener p_listener) throws TerminalException {
        m_sessionId = p_sessionId;
        m_socket = p_socket;
        m_listener = p_listener;

        try {
            m_out = new ObjectOutputStream(m_socket.getOutputStream());
            m_in = new ObjectInputStream(m_socket.getInputStream());
        } catch (final IOException e) {
            throw new TerminalException("Creating in/out streams for session failed", e);
        }
    }

    public byte getSessionId() {
        return m_sessionId;
    }

    public Object read() {
        try {
            return m_in.readObject();
        } catch (final IOException | ClassNotFoundException ignored) {
            System.out.println(ignored.getMessage());
            return null;
        }
    }

    public boolean write(final Object p_object) {
        try {
            m_out.writeObject(p_object);
            m_out.flush();
        } catch (final IOException ignored) {
            return false;
        } catch (final NoClassDefFoundError e) {
            // happens if terminal server is killed
            return false;
        }

        return true;
    }

    public void close() {
        // #if LOGGER == DEBUG
        LOGGER.debug("Closing session: %s", m_socket);
        // #endif /* LOGGER == DEBUG */

        m_in = null;
        m_out = null;

        try {
            m_socket.close();
        } catch (final IOException ignored) {
        }

        m_listener.sessionClosed(this);
    }

    @Override
    public String toString() {
        return m_socket.toString();
    }

    public interface Listener {
        void sessionClosed(final TerminalSession p_session);
    }
}
