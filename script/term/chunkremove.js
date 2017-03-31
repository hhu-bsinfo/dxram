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

function imports() {

}

function help() {
	return "Remove an existing chunk. Usable with either full chunk id or split into nid and lid\n" +
			"Usage (1): chunkremove(cidStr)\n" +
			"Usage (2): chunkremove(nid, lid)\n" +
			"  cidStr: Full chunk id of the chunk to remove as string\n" +
			"  nid: Node id to remove the chunk with specified local id\n" +
			"  lid: Local id of the chunk to remove. If missing node id, current node is assumed";
}

function exec(id1, id2) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified");
        return;
    }

    if (id2 == null) {
        execCid(dxram.longStrToLong(id1));
    } else {
        execCid(dxram.cid(id1, id2));
    }
}

function execCid(cid) {
    if (cid == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

    // don't allow removal of index chunk
    if (dxram.lidOfCid(cid) == 0) {
        dxterm.printlnErr("Removal of index chunk is not allowed")
        return;
    }

    var chunk = dxram.service("chunk");

    if (chunk.remove(cid) != 1) {
        dxterm.printflnErr("Removing chunk with ID 0x%X failed", cid);
    } else {
        dxterm.printfln("Chunk 0x%X removed", cid);
    }
}
