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
	return "List all available nodes or nodes of a specific type\n" +
			"Parameters: role\n" +
			"  role: Filter list by role if specified";
}

function exec(role) {

	var boot = dxram.service("boot");
	var nodeRole = dxram.nodeRole(role);

	var nodeIds = boot.getOnlineNodeIDs();

	if (role != null) {
		dxterm.printfln("Filtering by role %s", nodeRole);
	} 

	dxterm.printfln("Total available nodes (%d):", nodeIds.size());
		
	for each(nodeId in nodeIds) {
		var curRole = boot.getNodeRole(nodeId);
		
		if (role == null || role != null && nodeRole.equals(curRole)) {
			dxterm.printfln("\t0x%X, %s, %s", nodeId, curRole, nodeId);
		}
	}
}
