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
	return "Get a list of chunk id ranges from a peer holding chunks (migrated chunks optional)\n" +
			"Usage: chunklist(nid, migrated)\n" +
			"  nid: Node ID of the remote peer to get the list from\n" +
			"  migrated: List the migrated chunks as well (default: false)";
}

function exec(nid, migrated) {

	if (nid == null) {
		dxterm.printlnErr("No nid specified");
		return;
	}

	if (migrated == null) {
		migrated = false;
	}

	var chunk = dxram.service("chunk");

	var chunkRanges = chunk.getAllLocalChunkIDRanges(nid);

    if (chunkRanges == null) {
        dxterm.printlnErr("Getting chunk ranges failed");
        return;
    }

    dxterm.printfln("Locally created chunk id ranges of 0x%X (%d):", nid, java.lang.Integer.divideUnsigned(chunkRanges.size(), 2));

    for (var i = 0; i < chunkRanges.size(); i++) {
        var start = chunkRanges.getRangeStart(i);
        var end = chunkRanges.getRangeEnd(i);

        dxterm.println("[" + dxram.longToHexStr(start) + ", " + dxram.longToHexStr(end) + "]");
    }

    if (migrated) {
        chunkRanges = chunk.getAllMigratedChunkIDRanges(nid);

        if (chunkRanges == null) {
            dxterm.printlnErr("Getting migrated chunk ranges failed");
            return;
        }

        dxterm.printfln("Migrated chunk id ranges of 0x%X (%d):", nid, java.lang.Integer.divideUnsigned(chunkRanges.size(), 2));
        for (var i = 0; i < chunkRanges.size(); i++) {
            var start = chunkRanges.getRangeStart(i);
            var end = chunkRanges.getRangeEnd(i);

            dxterm.println("[" + dxram.longToHexStr(start) + ", " + dxram.longToHexStr(end) + "]");
        }
    }
}
