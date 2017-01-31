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
			"Usage: chunkstatus(nid)\n" +
			"  nid: Node ID of the remote peer to get the status from";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var chunk = dxram.service("chunk");

    var status = chunk.getStatus(nid);

    if (status == null) {
        dxterm.printlnErr("Getting status failed");
        return;
    }

    dxterm.println(status);
}
