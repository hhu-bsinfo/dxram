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
	return "Create a chunk on a remote node\n" +
			"Parameters: size nid\n" +
			"  size: Size of the chunk to create\n" +
			"  nid: Node id of the peer to create the chunk on";
}

function exec(size, nid) {

	if (size == null) {
		dxterm.printlnErr("No size specified");
		return;
	}
	
	if (nid == null) {
		dxterm.printlnErr("No nid specified");
		return;
	}

	var chunk = dxram.service("chunk");

	var chunkIDs = chunk.createRemote(nid, size);

	if (chunkIDs != null) {
	    dxterm.printfln("Created chunk of size %d: 0x%X", size, chunkIDs[0]);
	} else {
        dxterm.printlnErr("Creating chunk failed");
	}
}
