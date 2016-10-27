function imports() {

	importClass(Packages.de.hhu.bsinfo.dxram.data.Chunk);
}

function help() {

	return "Put data in the specified chunk (temporary storage)." +
            "If no offset is specified, the whole chunk is overwritten with the new data. " +
            "Otherwise the data is inserted at the starting offset with its length. " +
            "If the specified data is too long it will be trunced\n" +
			"Parameters: id data [offset] [type]\n" +
			"  id: Id of the chunk stored in temporary storage\n" +
			"  data: Data to store (format has to match type parameter)\n" +
			"  type: Type of the data to store (str, byte, short, int, long, hex), defaults to str\n" +
			"  offset: Offset within the existing to store the new data to. -1 to override existing data, defaults to 0";
}

function exec(id, data, offset, type) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
        return;
    }

    if (data == null) {
        dxterm.printlnErr("No data specified");
        return;
    }

    if (offset == null) {
        offset = 0;
    }

    if (type == null) {
        type = "str";
    }
    type = type.toLowerCase();

    var tmpstore = dxram.service("tmpstore");
    var chunk = tmpstore.get(id);

    if (chunk == null) {
        dxterm.printflnErr("Getting chunk 0x%X from tmp storage failed", id);
        return;
    }

    if (offset == -1) {
        // create new chunk
        chunk = new Chunk(chunk.getID(), chunk.getDataSize());
        offset = 0;
    }

    if (offset > chunk.sizeofObject()) {
        offset = chunk.sizeofObject();
    }

    var buffer = chunk.getData();
    buffer.position(offset);

    switch (type) {

        case "str":
            var bytes = data.getBytes(java.nio.charset.StandardCharsets.US_ASCII);

            try {
                var size = buffer.capacity() - buffer.position();
                if (bytes.length < size) {
                    size = bytes.length;
                }
                buffer.put(bytes, 0, size);
            } catch (e) {
                // that's fine, trunc data
            }
            break;

        case "byte":
            var b = java.lang.Integer.parseInt(data) & 0xFF;

            try {
                buffer.put(b);
            } catch (e) {
                // that's fine, trunc data
            }
            break;

        case "short":
            var v = java.lang.Integer.parseInt(data) & 0xFFFF;

            try {
                buffer.putShort(v);
            } catch (e) {
                // that's fine, trunc data
            }
            break;

        case "int":
            var v = java.lang.Integer.parseInt(data) & 0xFFFFFFFF;

            try {
                buffer.putInt(v);
            } catch (e) {
                // that's fine, trunc data
            }
            break;

        case "long":
            var v = java.lang.Long.parseLong(data) & 0xFFFFFFFFFFFFFFFF;

            try {
                buffer.putLong(v);
            } catch (e) {
                // that's fine, trunc data
            }
            break;

         case "hex":
            var tokens = data.split(" ");

            for each (var token in tokens) {
                var v = java.lang.Integer.parseInt(token, 16);
                try {
                    buffer.put(v);
                } catch (e) {
                    // that's fine, trunc data
                    break;
                }
            }
            break;

        default:
            dxterm.printflnErr("Unsupported data type %s", type);
            return;
    }

    // put chunk back
    if (tmpstore.put(chunk) != 1) {
        dxterm.printflnErr("Putting chunk 0x%X to tmp storage failed", id);
    } else {
        dxterm.printfln("Put to chunk 0x%X to tmp storage successful", id);
    }
}
