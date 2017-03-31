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

	return "Prints the log utilization of given peer\n" +
			"Usage: loginfo(nid)\n" +
			"  nid: Node id of the peer";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var log = dxram.service("log");

    dxterm.println(log.getCurrentUtilization(nid));
}
