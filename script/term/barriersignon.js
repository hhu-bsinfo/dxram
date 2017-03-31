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

	return "Sign on to an allocated barrier for synchronization (for testing/debugging)\n" +
			"Usage (1): barriersignon(bid)\n" +
			"Usage (2): barriersignon(bid, data)\n" +
			"  bid: Id of the barrier to sign on to\n" +
			"  data: Custom data to pass along with the sign on call (optional)";
}

function exec(bid, data) {

    if (bid == null) {
        dxterm.printlnErr("No bid specified");
        return;
    }

    if (data == null) {
        data = 0;
    }

    var sync = dxram.service("sync");
    var result = sync.barrierSignOn(bid, data);

    if (result == null) {
        dxterm.printlnErr("Signing on to barrier 0x%X failed", bid);
        return;
    }

    var str = "";
    for (var i = 0; i < result.first().length; i++) {
        str += "\n" + dxram.shortToHexStr(result.first()[i]) + ": " + dxram.longToHexStr(result.second()[i]);
    }

    dxterm.printfln("Synchronized to barrier 0x%X custom data: %s", bid, str)
}
