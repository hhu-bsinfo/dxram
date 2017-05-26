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

package de.hhu.bsinfo.dxterm.cmd;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Lock a chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunklock extends AbstractTerminalCommand {
    public TcmdChunklock() {
        super("chunklock");
    }

    @Override
    public String getHelp() {
        return "Lock a chunk\n" + "Usage (1): chunklock <cid>\n" + "Usage (2): chunklock <nid> <lid>\n" + "  cid: Full chunk ID of the chunk to lock\n" +
                "  nid: Separate local id part of the chunk to lock\n" + "  lid: Separate node id part of the chunk to lock";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        long cid;

        if (p_cmd.getArgc() > 1) {
            short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
            long lid = p_cmd.getArgLocalId(1, ChunkID.INVALID_ID);
            cid = ChunkID.getChunkID(nid, lid);
        } else {
            cid = p_cmd.getArgChunkId(0, ChunkID.INVALID_ID);
        }

        if (cid == ChunkID.INVALID_ID) {
            p_stdout.printlnErr("No or invalid cid specified");
            return;
        }

        // don't allow removal of index chunk
        if (ChunkID.getLocalID(cid) == 0) {
            p_stdout.printlnErr("Locking of index chunk is not allowed");
            return;
        }

        AbstractLockService.ErrorCode err = p_services.getService(AbstractLockService.class).lock(true, 1000, cid);
        if (err != AbstractLockService.ErrorCode.SUCCESS) {
            p_stdout.printflnErr("Error locking chunk 0x%X: %s", cid, err);
        } else {
            p_stdout.printfln("Locked chunk 0x%X", cid);
        }
    }
}
