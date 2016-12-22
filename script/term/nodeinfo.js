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
	return "Get information about either the current node or another node in the network\n" +
			"Usage (1): nodeinfo(nid)\n" +
			"Usage (2): nodeinfo()\n" +
			"  nid: If specified, gets information of this node";
}

function exec(nid) {
	var boot = dxram.service("boot");

	if (nid != null) {
		if (boot.nodeAvailable(nid)) {
			dxterm.println("Node info 0x%X:", nid);
			dxterm.println("\tRole: %s", boot.getNodeRole(nid));
			dxterm.println("\tAddress: %s", boot.getNodeAddress(nid));
		} else {
			dxterm.println("Not available.");
		}
	} else {
		dxterm.println("Node info 0x%X:", boot.getNodeID());
		dxterm.println("\tRole: %s", boot.getNodeRole());
		dxterm.println("\tAddress: %s", boot.getNodeAddress(boot.getNodeID()));
	}
}
