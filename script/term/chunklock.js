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
	return "Lock a chunk\n" +
			"Usage (1): chunklock(cidStr) | nid lid\n" +
			"Usage (2): chunklock(nid, lid)\n" +
			"  cidStr: Full chunk ID of the chunk to lock as string\n" +
			"  nid: Separate local id part of the chunk to lock\n" +
			"  lid: Separate node id part of the chunk to lock";
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

    // don't allow lock of index chunk
    if (dxram.lidOfCid(cid) == 0) {
        dxterm.printlnErr("Locking of index chunk is not allowed")
        return;
    }

    var err = dxram.service("lock").lock(true, 1000, cid);
    if (!err.toString().equals("SUCCESS")) {
        dxterm.printflnErr("Error locking chunk 0x%X: %s", cid, err);
    } else {
        dxterm.printfln("Locked chunk 0x%X");
    }
}
