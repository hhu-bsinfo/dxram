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

package de.hhu.bsinfo.dxram.chunk.tcmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Dump the contents of a chunk to a file
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.04.2017
 */
public class TcmdChunkdump extends AbstractTerminalCommand {
    public TcmdChunkdump() {
        super("chunkdump");
    }

    @Override
    public String getHelp() {
        return "Dump the contents of a chunk to a file\n" + " Usage (1): chunkdump <cid> <fileName>\n" + " Usage (2): chunkdump <nid> <lid> <fileName\n" +
            "  cid: Full chunk ID of the chunk to dump\n" + "  nid: Separate node id part of the chunk to dump\n" +
            "  lid: (In combination with) separate local id part of the chunk to dump\n" +
            "  fileName: File to dump the contents to (existing file gets deleted)";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        long cid;
        String fileName;

        if (p_args.length < 1) {
            p_ctx.printlnErr("No cid specified");
            return;
        }

        if (p_ctx.isArgChunkID(p_args, 0)) {
            cid = p_ctx.getArgChunkId(p_args, 0, ChunkID.INVALID_ID);
            fileName = p_ctx.getArgString(p_args, 1, null);
        } else {
            short nid = p_ctx.getArgNodeId(p_args, 0, NodeID.INVALID_ID);
            long lid = p_ctx.getArgLocalId(p_args, 1, ChunkID.INVALID_ID);

            if (lid == ChunkID.INVALID_ID) {
                p_ctx.printlnErr("No lid specified");
                return;
            }

            cid = ChunkID.getChunkID(nid, lid);

            fileName = p_ctx.getArgString(p_args, 2, null);
        }

        if (cid == ChunkID.INVALID_ID) {
            p_ctx.printlnErr("No cid specified");
            return;
        }

        if (fileName == null) {
            p_ctx.printlnErr("No file name specified");
            return;
        }

        ChunkAnonService chunkAnon = p_ctx.getService(ChunkAnonService.class);

        ChunkAnon[] chunks = new ChunkAnon[1];
        if (chunkAnon.get(chunks, cid) != 1) {
            p_ctx.printflnErr("Getting chunk 0x%X failed: %s", cid, chunks[0].getState());
            return;
        }

        ChunkAnon chunk = chunks[0];

        File file = new File(fileName);

        if (file.exists()) {
            file.delete();
        }

        p_ctx.printfln("Dumping chunk 0x%X to file %s...", cid, fileName);

        RandomAccessFile raFile;
        try {
            raFile = new RandomAccessFile(file, "rw");
        } catch (final FileNotFoundException ignored) {
            p_ctx.printlnErr("Dumping chunk failed, file not found");
            return;
        }

        try {
            raFile.write(chunk.getData());
        } catch (final IOException e) {
            p_ctx.printflnErr("Dumping chunk failed: %s", e.getMessage());
            return;
        }

        try {
            raFile.close();
        } catch (final IOException ignore) {

        }

        p_ctx.printfln("Chunk dumped");
    }
}
