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

function imports() {

}

function help() {
	return "Get the status of the chunk service/memory from a remote node\n" +
			"Usage: chunkstatus(nid, sizetype)\n" +
			"  nid: Node ID of the remote peer to get the status from\n" +
			"  sizetype: Specify the type of size you want to display (b, kb, mb, gb)";
}

function exec(nid, sizetype) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    if (sizetype == null) {
        sizetype = "b";
    }

    var chunk = dxram.service("chunk");

    var status = chunk.getStatus(nid);

    if (status == null) {
        dxterm.printlnErr("Getting status failed");
        return;
    }

    var divisor = 1;
    if (sizetype != null) {
        sizetype = sizetype.toLowerCase();
        if (sizetype.equals("b")) {
            divisor = 1;
        } else if (sizetype.equals("kb")) {
            divisor = 1024;
        } else if (sizetype.equals("mb")) {
            divisor = 1024 * 1024;
        } else if (sizetype.equals("gb")) {
            divisor = 1024 * 1024 * 1024;
        } else {
            // invalid, default to byte
            sizetype = "b";
        }
    } else {
        sizetype = "b";
    }

    dxterm.printfln("Chunk service/memory status of 0x%X:", nid);
    if (divisor == 1) {
        dxterm.printfln("Free memory (%s): %d", sizetype, status.getFreeMemory());
        dxterm.printfln("Total memory (%s): %d", sizetype, status.getTotalMemory());
        dxterm.printfln("Total payload memory (%s): %d", sizetype, status.getTotalPayloadMemory());
        dxterm.printfln("Total chunk payload memory (%s): %d", sizetype, status.getTotalChunkPayloadMemory());
        dxterm.printfln("Total CID tables memory (NID table with 327687) (%s): %d", sizetype,
                        status.getTotalMemoryCIDTables());
    } else {
        dxterm.printfln("Free memory (%s): %d", sizetype, status.getFreeMemory() / divisor);
        dxterm.printfln("Total memory (%s): %d", sizetype, status.getTotalMemory() / divisor);
        dxterm.printfln("Total payload memory (%s): %d", sizetype, status.getTotalPayloadMemory() / divisor);
        dxterm.printfln("Total chunk payload memory (%s): %d", sizetype,
                        status.getTotalChunkPayloadMemory() / divisor);
        dxterm.printfln("Total CID tables memory (NID table with 327687) (%s): %d",
                        sizetype, status.getTotalMemoryCIDTables() / divisor);
    }

    dxterm.printfln("Num active memory blocks: %d", status.getNumberOfActiveMemoryBlocks());
    dxterm.printfln("Num active chunks: %d", status.getNumberOfActiveChunks());
    dxterm.printfln("Num CID tables (one is NID table): %d", status.getCIDTableCount());
}
