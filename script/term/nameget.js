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

	return "Register a nameservice entry for a specific chunk id\n" +
			"Usage: nameget(name)\n" +
			"  name: Name to get the chunk id for";
}

function exec(name) {

    if (name == null) {
        dxterm.printlnErr("No name specified");
        return;
    }

    var nameservice = dxram.service("name");

    var cid = nameservice.getChunkID(name, 2000);

    if (cid == -1) {
        dxterm.printflnErr("Could not get name entry for %s, does not exist", name);
    } else {
        dxterm.printfln("%s: 0x%X", name, cid);
    }
}
