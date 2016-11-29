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
	return "Prints the look up tree of a specified node\n" +
			"Parameters: nid\n" +
			"  nid: Node id of the peer to print the lookup tree of";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var lookup = dxram.service("lookup");

    var respSuperpeer = lookup.getResponsibleSuperpeer(nid);

    if (respSuperpeer == -1) {
        dxterm.printflnErr("No responsible superpeer for 0x%X found", nid);
        return;
    }

    var tree = lookup.getLookupTreeFromSuperpeer(respSuperpeer, nid);
    dxterm.printfln("Lookup tree of 0x%X:\n%s", nid, tree);
}
