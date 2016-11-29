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

	return "Create a new barrier for synchronization of mutliple peers\n" +
			"Parameters: size\n" +
			"  size: Size of the barrier, i.e. the number of peers that have to sign on for release";
}

function exec(size) {

    if (size == null) {
        dxterm.printlnErr("No size specified");
        return;
    }

    var sync = dxram.service("sync");

    var barrierId = sync.barrierAllocate(size);
    if (barrierId == -1) {
        dxterm.printlnErr("Allocating barrier failed");
    } else {
        dxterm.printfln("Allocating barrier successful, barrier id: 0x%X", barrierId);
    }
}
