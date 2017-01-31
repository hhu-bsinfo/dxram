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

	return "Get a chunk specified by either full cid or separted lid + nid from a storage\n" +
            "Usage (1): chunkget(cidStr)\n" +
            "Usage (2): chunkget(nid, lid)\n" +
			"Usage (3): chunkget(cidStr, className)\n" +
			"Usage (4): chunkget(nid, lid, className)\n" +
			"Usage (5): chunkget(cidStr, type, hex, offset, length)\n" +
			"Usage (6): chunkget(cidStr, type, hex, offset)\n" +
			"Usage (7): chunkget(cidStr, type, hex)\n" +
			"Usage (8): chunkget(cidStr, type)\n" +
            "Usage (9): chunkget(nid, lid, type, hex, offset, length)\n" +
            "Usage (10): chunkget(nid, lid, type, hex, offset)\n" +
            "Usage (11): chunkget(nid, lid, type, hex)\n" +
            "Usage (12): chunkget(nid, lid, type)\n" +
            "Usage (13): chunkget(cidStr, offset, length, type, hex)\n" +
            "Usage (14): chunkget(cidStr, offset, length, type)\n" +
            "Usage (15): chunkget(cidStr, offset, length)\n" +
            "Usage (16): chunkget(cidStr, offset)\n" +
            "Usage (18): chunkget(nid, lid, offset, length, type, hex)\n" +
            "Usage (19): chunkget(nid, lid, offset, length, type)\n" +
            "Usage (20): chunkget(nid, lid, offset, length)\n" +
            "Usage (21): chunkget(nid, lid, offset)\n" +
			"  cidStr: Full chunk ID of the chunk to get data from (as string!)\n" +
			"  nid: (Or) separate node id part of the chunk to get data from\n" +
			"  lid: (In combination with) separate local id part of the chunk to get data from\n" +
			"  className: Full name of a java class that implements DataStructure (with package path) as string.\n" +
			"An instance is created, the data is stored in that instance and printed.\n " +
			"  type: Format to print the data (\"str\", \"byte\", \"short\", \"int\", \"long\"), defaults to \"byte\"\n"  +
			"  hex: For some representations, print as hex instead of decimal, defaults to true\n" +
			"  offset: Offset within the chunk to start getting data from, defaults to 0\n" +
			"  length: Number of bytes of the chunk to print, defaults to size of chunk";
}

// ugly way to support overloading and type dispatching
function exec() {

    if (arguments.length > 0 && typeof arguments[0] === "string") {
        exec_cid.apply(this, arguments);
    } else if (arguments.length > 1) {
        exec_nidlid.apply(this, arguments);
    } else {
        dxterm.printlnErr("No cid or nid|lid specified");
    }
}

function exec_nidlid(nid, lid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    if (lid == null) {
        dxterm.printlnErr("No lid specified");
        return;
    }

    exec_cid.apply(this, [dxram.cid(nid, lid)].concat(Array.prototype.slice.call(arguments, 2)));
}

function exec_cid(cid) {

    if (cid == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

	cid = dxram.longStrToLong(cid);

    if (typeof arguments[1] === "string" && arguments.length == 2) {
        exec_class(cid, arguments[1]);
    } else if (typeof arguments[1] === "string") {
        exec_raw.apply(this, arguments);
    } else {
        exec_raw2.apply(this, arguments);
    }
}

function exec_class(cid, className) {

    if (cid == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

    if (className == null) {
        dxterm.printlnErr("No className specified");
        return;
    }

    var dataStructure = dxram.newDataStructure(className);
    if (dataStructure == null) {
        dxterm.printflnErr("Creating data structure of name '%s' failed", className);
        return;
    }

    dataStructure.setID(cid);

    var chunk = dxram.service("chunk");

    if (chunk.get(dataStructure) != 1) {
        dxterm.printflnErr("Getting data structure 0x%X failed", cid);
        return;
    }

    dxterm.printfln("DataStructure %s (size %d): %s", className, dataStructure.sizeofObject(), dataStructure);
}

function exec_raw(cid, type, hex, offset, length) {

    if (cid == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

    if (offset == null) {
        offset = 0;
    }

    if (type == null) {
        type = "byte";
    }
    type = type.toLowerCase();

    if (hex == null) {
        hex = true;
    }

    var chunkService = dxram.service("chunk");

    var chunks = chunkService.get(cid);

    if (chunks == null || chunks[0].getDataSize() == 0) {
        dxterm.printflnErr("Getting chunk 0x%X failed", cid);
        return;
    }

    var chunk = chunks[0];

    if (length == null || length > chunk.getDataSize()) {
        length = chunk.getDataSize();
    }

    if (offset > length) {
        offset = length;
    }

    if (offset + length > chunk.getDataSize()) {
        length = chunk.getDataSize() - offset;
    }

    var buffer = chunk.getData();
    buffer.position(offset);

    var str = "";
    switch (type) {
        case "str":
            str = new java.lang.String(buffer.array(), offset, length, java.lang.StandardCharsets.US_ASCII);
            break;

        case "byte":
            for (var i = 0; i < length; i += java.lang.Byte.BYTES) {
                if (hex) {
                    str += java.lang.Integer.toHexString(buffer.get() & 0xFF) + " ";
                } else {
                    str += buffer.get() + " ";
                }
            }
            break;

        case "short":
            for (var i = 0; i < length; i += java.lang.Short.BYTES) {
                if (hex) {
                    str += java.lang.Integer.toHexString(buffer.getShort() & 0xFFFF) + " ";
                } else {
                    str += buffer.getShort() + " ";
                }
            }
            break;

        case "int":
            for (var i = 0; i < length; i += java.lang.Integer.BYTES) {
                if (hex) {
                    str += java.lang.Integer.toHexString(buffer.getInt() & 0xFFFFFFFF) + " ";
                } else {
                    str += buffer.getInt() + " ";
                }
            }
            break;

        case "long":
            for (var i = 0; i < length; i += java.lang.Long.BYTES) {
                if (hex) {
                    str += java.lang.Long.toHexString(buffer.getLong() & new java.lang.Long(0xFFFFFFFFFFFFFFFF)) + " ";
                } else {
                    str += buffer.getLong() + " ";
                }
            }
            break;

        default:
            dxterm.printflnErr("Unsuported data type %s", type);
            return;
    }

    dxterm.printfln("Chunk data of 0x%X (chunksize %d): \n%s", cid, chunk.sizeofObject(), str);
}

function exec_raw2(cid, offset, length, type, hex) {

    exec_raw(cid, type, hex, offset, length);
}
