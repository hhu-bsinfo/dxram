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
	return "Create a full memory dump of the chunk memory\n" +
			"Usage: chunkdump(nid, fileName)\n" +
			"  nid: Node ID of the remote peer to create the dump of\n" +
			"  fileName: Name of the file to dump the memory to";
}

function exec(nid, fileName) {

	if (nid == null) {
		dxterm.printlnErr("No nid specified");
		return;
	}

	if (fileName == null) {
		dxterm.printlnErr("No file name specified");
		return;
	}

	var chunk = dxram.service("chunk");

    dxterm.printfln("Trigger dumping memory of 0x%X to file %s...", nid, fileName);
	chunk.dumpChunkMemory(fileName, nid);
	dxterm.println("(Async) Dumping to memory triggered, depending on the memory size, this might take a few seconds");
}
