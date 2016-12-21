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
	return "Migrate a chunk from one peer to another one\n" +
			"Usage (1): chunkmigrate(cidStr, targetNid)\n" +
			"Usage (2): chunkmigrate(nid, lid, targetNid)\n" +
            "  cidStr: Full chunk ID of the chunk to migrate as string\n" +
            "  nid: Separate local id part of the chunk to migrate\n" +
            "  lid: Separate node id part of the chunk to migrate\n" +
            "  targetNid: Target node id to migrate the chunk to";
}

function exec(id1, id2, targetNid) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified");
        return;
    }

    if (targetNid == null) {
        dxterm.printlnErr("No targetNid specified");
        return;
    }

    if (id2 == null) {
        execCid(dxram.longStrToLong(id1), targetNid);
    } else {
        execCid(dxram.cid(id1, id2), targetNid);
    }
}

function execCid(cid, targetNid) {

    if (cid == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

    // don't allow migration of index chunk
    if (dxram.lidOfCid(cid) == 0) {
        dxterm.printlnErr("Migration of index chunk is not allowed")
        return;
    }

    var migration = dxram.service("migration");

    // TODO error handling
    migration.targetMigrate(cid, targetNid);
}
