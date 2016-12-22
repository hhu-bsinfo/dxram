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
	return "Get the list of all locked chunks of a node\n" +
			"Usage: chunklocklist(nid)\n" +
			"  nid: Get the list of locked chunks from a remote node";
}

function exec(nid, id2) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var boot = dxram.service("boot");
    var lock = dxram.service("lock");

    var list = lock.getLockedList(nid);

    if (list == null) {
       dxterm.printlnErr("Getting list of locked chunks failed");
       return;
    }

    dxterm.printfln("Locked chunks of 0x%X (%d):", nid, list.size());
    dxterm.println("<lid: nid that locked the chunk>");
    for each (entry in list) {
        dxterm.printfln("0x%X: 0x%X", entry.first(), entry.second());
    }
}
