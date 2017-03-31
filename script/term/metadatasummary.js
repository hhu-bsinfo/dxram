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
	return "Prints a summary of the specified superpeer's metadata\n" +
			"Usage: metadatasummary(nid)\n" +
			"  nid: Node id of the superpeer to print the metadata of\n" +
			"       \"all\" prints metadata of all superpeers";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var lookup = dxram.service("lookup");
    if (nid == "all") {
        var boot = dxram.service("boot");
        var nodeIds = boot.getOnlineNodeIDs();

        for each(nodeId in nodeIds) {
            var curRole = boot.getNodeRole(nodeId);
            if (curRole == "superpeer") {
                var summary = lookup.getMetadataSummary(nodeId);
                dxterm.printfln("Metadata summary of 0x%X:\n%s", nodeId, summary);
            }
        }
    } else {
        var summary = lookup.getMetadataSummary(nid);
        dxterm.printfln("Metadata summary of 0x%X:\n%s", nid, summary);
    }
}
