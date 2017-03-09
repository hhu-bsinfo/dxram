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

	return "Get the chunk id for a registered name mapping\n" +
			"Usage (1): namereg(cidStr, name)\n" +
			"Usage (2): namereg(nid, lid, name)\n" +
            "  cidStr: Full chunk ID of the chunk to register as string\n" +
            "  nid: Separate local id part of the chunk to register\n" +
            "  lid: Separate node id part of the chunk to register\n" +
            "  name: Name to register the chunk id for";
}

function exec(id1, id2, name) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified");
        return;
    }

    if (name == null) {
        execCid(dxram.longStrToLong(id1), id2);
    } else {
        execCid(dxram.cid(id1, id2), name);
    }
}

function execCid(cid, name) {

   if (name == null) {
        dxterm.printlnErr("No name specified");
        return;
    }

    dxram.service("name").register(cid, name);
}
