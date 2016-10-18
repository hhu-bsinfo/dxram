function help() {

	return "Get a chunk specified by either full cid or separted lid + nid from a storage\n" +
			"Parameters (1): cidStr or nid|lid className\n" +
			"Parameters (2): cidStr or nid|lid [type] [hex] [offset] [length]\n" +
			"Parameters (3): cidStr or nid|lid [offset] [length] [type] [hex]\n" +
			"  cidStr: Full chunk ID of the chunk to get data from (as string!)\n" +
			"  nid: (Or) separate node id part of the chunk to get data from\n" +
			"  lid: (In combination with) separate local id part of the chunk to get data from\n" +
			"  className: Full name of a java class that implements DataStructure (with package path). " +
			"An instance is created, the data is stored in that instance and printed.\n " +
			"  type: Format to print the data (str, byte, short, int, long), defaults to byte\n"  +
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
        dxterm.printlnErr("Creating data structure of name '" + className + "' failed");
        return;
    }

    dataStructure.setID(cid);

    var chunk = dxram.service("chunk");

    if (chunk.get(dataStructure) != 1) {
        dxterm.printlnErr("Getting data structure " + dxram.cidHexStr(cid) + " failed.");
        return;
    }

    dxterm.println("DataStructure " + className + " (size " + dataStructure.sizeofObject() + "): ");
    dxterm.println(dataStructure);
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

    if (chunks == null || chunks.first() == 0) {
        dxterm.printlnErr("Getting chunk " + dxram.cidHexStr(cid) + " failed.");
        return;
    }

    var chunk = chunks.second()[0];

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
            dxterm.printlnErr("Unsuported data type " + type);
            return;
    }

    dxterm.println("Chunk data of " + dxram.cidHexStr(cid) + " (chunksize " + chunk.sizeofObject() + "):");
    dxterm.println(str);
}

function exec_raw2(cid, offset, length, type, hex) {

    exec_raw(cid, type, hex, offset, length);
}
