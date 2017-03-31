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

	return "Get a list of available compute groups\n" +
			"Usage: compgrpls()";
}

function exec(nid) {

    var mscomp = dxram.service("mscomp");
    var masters = mscomp.getMasters();

    dxterm.printfln("List of available compute groups with master nodes (%d):", masters.size())
    for each (master in masters) {
        dxterm.printfln("%d: 0x%X", master.second(), master.first());
    }
}
