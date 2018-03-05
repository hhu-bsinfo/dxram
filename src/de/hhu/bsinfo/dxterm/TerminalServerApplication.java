/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.chunk.ChunkDebugService;
import de.hhu.bsinfo.dxram.chunk.ChunkRemoveService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxterm.cmd.TcmdBarrieralloc;
import de.hhu.bsinfo.dxterm.cmd.TcmdBarrierfree;
import de.hhu.bsinfo.dxterm.cmd.TcmdBarriersignon;
import de.hhu.bsinfo.dxterm.cmd.TcmdBarriersize;
import de.hhu.bsinfo.dxterm.cmd.TcmdBarrierstatus;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkMigrate;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkcreate;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkdump;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkget;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunklist;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunklock;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunklocklist;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkput;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkremove;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkremoverange;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkstatus;
import de.hhu.bsinfo.dxterm.cmd.TcmdChunkunlock;
import de.hhu.bsinfo.dxterm.cmd.TcmdCompgrpls;
import de.hhu.bsinfo.dxterm.cmd.TcmdCompgrpstatus;
import de.hhu.bsinfo.dxterm.cmd.TcmdComptask;
import de.hhu.bsinfo.dxterm.cmd.TcmdComptaskscript;
import de.hhu.bsinfo.dxterm.cmd.TcmdLoggerlevel;
import de.hhu.bsinfo.dxterm.cmd.TcmdLoginfo;
import de.hhu.bsinfo.dxterm.cmd.TcmdLookuptree;
import de.hhu.bsinfo.dxterm.cmd.TcmdMemdump;
import de.hhu.bsinfo.dxterm.cmd.TcmdMetadata;
import de.hhu.bsinfo.dxterm.cmd.TcmdNameget;
import de.hhu.bsinfo.dxterm.cmd.TcmdNamelist;
import de.hhu.bsinfo.dxterm.cmd.TcmdNamereg;
import de.hhu.bsinfo.dxterm.cmd.TcmdNodeinfo;
import de.hhu.bsinfo.dxterm.cmd.TcmdNodelist;
import de.hhu.bsinfo.dxterm.cmd.TcmdNodeshutdown;
import de.hhu.bsinfo.dxterm.cmd.TcmdNodewait;
import de.hhu.bsinfo.dxterm.cmd.TcmdStatsprint;
import de.hhu.bsinfo.dxterm.cmd.TcmdTmpcreate;
import de.hhu.bsinfo.dxterm.cmd.TcmdTmpget;
import de.hhu.bsinfo.dxterm.cmd.TcmdTmpput;
import de.hhu.bsinfo.dxterm.cmd.TcmdTmpremove;
import de.hhu.bsinfo.dxterm.cmd.TcmdTmpstatus;

/**
 * Terminal server running on a DXRAM peer as a DXRAM application. Thin clients can connect to the server and execute
 * terminal commands on the peer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TerminalServerApplication extends AbstractApplication implements TerminalSession.Listener,
        TerminalServiceAccessor {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalServerApplication.class.getSimpleName());

    @Expose
    private final int m_port = 22220;
    @Expose
    private final int m_maxSessions = 1;

    private TerminalServer m_terminalServer;

    private ServerSocket m_socket;
    private ExecutorService m_threadPool;

    private volatile boolean m_run = true;
    private List<TerminalSession> m_sessions = Collections.synchronizedList(new ArrayList<>());

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return DXRAM.VERSION;
    }

    @Override
    public String getApplicationName() {
        return "TerminalServer";
    }

    @Override
    public boolean useConfigurationFile() {
        return true;
    }

    @Override
    public void main() {
        short nodeId = getService(BootService.class).getNodeID();

        m_terminalServer = new TerminalServer(nodeId);
        registerTerminalCommands();

        m_threadPool = Executors.newFixedThreadPool(m_maxSessions);

        try {
            m_socket = new ServerSocket(m_port);
            m_socket.setSoTimeout(1000);
        } catch (final IOException e) {
            // #if LOGGER == ERROR
            LOGGER.error("Creating server socket failed", e);
            // #endif /* LOGGER == ERROR */
            return;
        }

        while (m_run) {
            if (m_sessions.size() == m_maxSessions) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {

                }

                continue;
            }

            Socket sock;

            try {
                sock = m_socket.accept();
            } catch (final SocketTimeoutException ignored) {
                // accept timeout, just continue
                continue;
            } catch (final IOException e) {
                // #if LOGGER == ERROR
                LOGGER.error("Accepting client connection failed", e);
                // #endif /* LOGGER == ERROR */
                continue;
            }

            // #if LOGGER == DEBUG
            LOGGER.debug("Accepted connection: %s", sock);
            // #endif /* LOGGER == DEBUG */

            TerminalSession session;
            try {
                session = new TerminalSession((byte) m_sessions.size(), sock, this);
            } catch (final TerminalException e) {
                // #if LOGGER == ERROR
                LOGGER.error("Creating terminal session failed", e);
                // #endif /* LOGGER == ERROR */

                try {
                    sock.close();
                } catch (final IOException ignored) {

                }

                continue;
            }

            // #if LOGGER == INFO
            LOGGER.info("Created terminal client session: %s", session);
            // #endif /* LOGGER == INFO */

            m_sessions.add(session);
            m_threadPool.submit(new TerminalServerSession(m_terminalServer, session, this));

            if (m_sessions.size() == m_maxSessions) {
                // #if LOGGER == DEBUG
                LOGGER.debug("Max session limit (%d) reached, further sessions won't be accepted", m_maxSessions);
                // #endif /* LOGGER == DEBUG */
            }
        }
    }

    @Override
    public void signalShutdown() {
        m_run = false;
    }

    @Override
    public void sessionClosed(final TerminalSession p_session) {
        m_sessions.remove(p_session);
    }

    @Override
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return super.getService(p_class);
    }

    /**
     * Register all available terminal commands
     */
    private void registerTerminalCommands() {
        if (isServiceAvailable(BootService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdNodeinfo());
            m_terminalServer.registerTerminalCommand(new TcmdNodelist());
            m_terminalServer.registerTerminalCommand(new TcmdNodeshutdown());
            m_terminalServer.registerTerminalCommand(new TcmdNodewait());
        }

        if (isServiceAvailable(ChunkAnonService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdChunkget());
            m_terminalServer.registerTerminalCommand(new TcmdChunkput());
        }

        if (isServiceAvailable(ChunkDebugService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdMemdump());
        }

        if (isServiceAvailable(ChunkService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdChunkcreate());
            m_terminalServer.registerTerminalCommand(new TcmdChunkdump());
            m_terminalServer.registerTerminalCommand(new TcmdChunklist());
            m_terminalServer.registerTerminalCommand(new TcmdChunkstatus());
        }

        if (isServiceAvailable(ChunkRemoveService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdChunkremove());
            m_terminalServer.registerTerminalCommand(new TcmdChunkremoverange());
        }

        if (isServiceAvailable(AbstractLockService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdChunklock());
            m_terminalServer.registerTerminalCommand(new TcmdChunklocklist());
            m_terminalServer.registerTerminalCommand(new TcmdChunkunlock());
        }

        if (isServiceAvailable(LogService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdLoginfo());
        }

        if (isServiceAvailable(LoggerService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdLoggerlevel());
        }

        if (isServiceAvailable(LookupService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdLookuptree());
            m_terminalServer.registerTerminalCommand(new TcmdMetadata());
        }

        if (isServiceAvailable(MasterSlaveComputeService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdCompgrpls());
            m_terminalServer.registerTerminalCommand(new TcmdCompgrpstatus());
            m_terminalServer.registerTerminalCommand(new TcmdComptask());
            m_terminalServer.registerTerminalCommand(new TcmdComptaskscript());
        }

        if (isServiceAvailable(MigrationService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdChunkMigrate());
        }

        if (isServiceAvailable(NameserviceService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdNameget());
            m_terminalServer.registerTerminalCommand(new TcmdNamelist());
            m_terminalServer.registerTerminalCommand(new TcmdNamereg());
        }

        if (isServiceAvailable(StatisticsService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdStatsprint());
        }

        if (isServiceAvailable(SynchronizationService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdBarrieralloc());
            m_terminalServer.registerTerminalCommand(new TcmdBarrierfree());
            m_terminalServer.registerTerminalCommand(new TcmdBarriersignon());
            m_terminalServer.registerTerminalCommand(new TcmdBarriersize());
            m_terminalServer.registerTerminalCommand(new TcmdBarrierstatus());
        }

        if (isServiceAvailable(TemporaryStorageService.class)) {
            m_terminalServer.registerTerminalCommand(new TcmdTmpcreate());
            m_terminalServer.registerTerminalCommand(new TcmdTmpget());
            m_terminalServer.registerTerminalCommand(new TcmdTmpput());
            m_terminalServer.registerTerminalCommand(new TcmdTmpremove());
            m_terminalServer.registerTerminalCommand(new TcmdTmpstatus());
        }
    }
}
