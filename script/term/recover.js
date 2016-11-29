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

	return "Recovers all data of owner on dest\n" +
			"Parameters: ownerNid destNid\n" +
			"  ownerNid: Node id of the owner to recover data of\n" +
			"  destNid: Destination node id to recover the data to";
}

function exec(ownerNid, destNid) {

    if (ownerNid == null) {
        dxterm.printlnErr("No ownerNid specified");
        return;
    }

    if (destNid == null) {
        dxterm.printlnErr("No destNid specified");
        return;
    }

    dxram.service("recovery").recover(ownerNid, destNid, true);
}
