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
	return "Shutdown a DXRAM node\n" +
			"Usage (1): nodeshutdown(nid)\n" +
			"Usage (2): nodeshutdown(nid, kill)\n" +
			"  nid: Id of the node to shutdown\n" +
			"  kill: If specified, true for a hard shutdown (kill process), false for proper soft shutdown (default)"
}

function exec(nid, kill) {
	var boot = dxram.service("boot");

	if (nid == null) {
		dxterm.printlnErr("No nodeID specified");
		return;
	}

	if (kill == null) {
		kill = false;
	}

	if (!boot.shutdownNode(nid, kill)) {
		dxterm.printfln("Shutting down node 0x%X failed", nid);
	} else {
		dxterm.printfln("Shutting down node 0x%X...", nid);
		dxterm.printfln("Ignore ERROR messages and continue by pressing <Enter>");
	}
}
