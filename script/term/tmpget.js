function help() {

	return "Get a chunk from the temporary storage\n" +
			"Parameters (1): id className\n" +
			"Parameters (2): id [type] [hex] [offset] [length]\n" +
			"Parameters (3): id [offset] [length] [type] [hex]\n" +
			"  id: Id of the chunk stored in temporary storage\n" +
			"  className: Full name of a java class that implements DataStructure (with package path). " +
			"An instance is created, the data is stored in that instance and printed.\n " +
			"  type: Format to print the data (str, byte, short, int, long), defaults to byte\n"  +
			"  hex: For some representations, print as hex instead of decimal, defaults to true\n" +
			"  offset: Offset within the chunk to start getting data from, defaults to 0\n" +
			"  length: Number of bytes of the chunk to print, defaults to size of chunk";
}

// ugly way to support overloading and type dispatching
function exec(id) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
        return;
    }

    if (typeof arguments[1] === "string" && arguments.length == 2) {
        exec_class(id, arguments[1]);
    } else if (typeof arguments[1] === "string") {
        exec_raw.apply(this, arguments);
    } else {
        exec_raw2.apply(this, arguments);
    }
}

function exec_class(id, className) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
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

    dataStructure.setID(id);

    var tmpstore = dxram.service("tmpstore");

    if (tmpstore.get(dataStructure) != 1) {
        dxterm.printlnErr("Getting tmp data structure " + dxram.intToHexStr(id) + " failed.");
        return;
    }

    dxterm.println("DataStructure " + className + " (size " + dataStructure.sizeofObject() + "): ");
    dxterm.println(dataStructure);
}

function exec_raw(id, type, hex, offset, length) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
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

    var tmpstore = dxram.service("tmpstore");

    var chunk = tmpstore.get(id);

    if (chunk == null) {
        dxterm.printlnErr("Getting tmp chunk " + dxram.intToHexStr(id) + " failed.");
        return;
    }

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

    dxterm.println("Temp chunk data of " + dxram.intToHexStr(id) + " (chunksize " + chunk.sizeofObject() + "):");
    dxterm.println(str);
}

function exec_raw2(id, offset, length, type, hex) {

    exec_raw(id, type, hex, offset, length);
}
