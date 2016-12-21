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

	return "Change the size of an existing barrier, keeping its id\n" +
			"Usage: barriersize(bid, size)\n" +
			"  bid: Id of the barrier to change its size\n" +
			"  size: New size for the existing barrier";
}

function exec(bid, size) {

    if (bid == null) {
        dxterm.printlnErr("No bid specified");
        return;
    }

    if (size == null) {
        dxterm.printlnErr("No size specified");
        return;
    }

    var sync = dxram.service("sync");

    if (!sync.barrierChangeSize(bid, size)) {
        dxterm.printflnErr("Changing barrier 0x%X size to %d failed", bid, size);
    } else {
        dxterm.printfln("Barrier 0x%X size changed to %d", bid, size);
    }
}
