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

	return "Get the current status of a barrier\n" +
			"Parameters: bid\n" +
			"  bid: Id of an allocated barrier to get the status of";
}

function exec(bid) {

    if (bid == null) {
        dxterm.printlnErr("No bid specified");
        return;
    }

    var sync = dxram.service("sync");
    var status = sync.barrierGetStatus(bid);

    if (status == null) {
        dxterm.printflnErr("Getting status of barrier 0x%X failed", bid);
        return;
    }

    var peers = "";
    for (var i = 1; i < status.length; i++) {
        peers += dxram.shortToHexStr(status[i]) + ", ";
    }

    dxterm.printfln("Barrier status 0x%X, %s/%d: %d", bid, status[0], (status.length - 1), peers);
}
