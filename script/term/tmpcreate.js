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

	return "Allocate memory for a chunk on a superpeer's storage (temporary)\n" +
			"Parameters: id size\n" +
			"  id: Id to identify the chunk in the storage\n" +
			"  size: Size of the chunk to create";
}

function exec(id, size) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
        return;
    }

    if (size == null) {
        dxterm.printlnErr("No size specified");
        return;
    }

    var tmpstore = dxram.service("tmpstore");

    if (tmpstore.create(id, size)) {
        dxterm.printfln("Created chunk of size %d in temporary storage: 0x%X", size, id);
    } else {
        dxterm.printlnErr("Creating chunk in temporary storage failed");
    }
}
