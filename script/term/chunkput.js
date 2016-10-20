function imports() {

	importClass(Packages.de.hhu.bsinfo.dxram.data.Chunk);
}

function help() {

	return "Put data in the specified chunk. Either use a full cid or separete nid + lid to specify the chunk id. " +
            "If no offset is specified, the whole chunk is overwritten with the new data. " +
            "Otherwise the data is inserted at the starting offset with its length. " +
            "If the specified data is too long it will be trunced\n" +
			"Parameters: cidStr or nid|lid data [offset] [type]\n" +
			"  cidStr: Full chunk ID of the chunk to put data to (as string!)\n" +
			"  nid: (Or) separate local id part of chunk to put the data to" +
			"  lid: (In combination with) separate node id part of the chunk to put the data to\n" +
			"  data: Data to store (format has to match type parameter)\n" +
			"  type: Type of the data to store (str, byte, short, int, long, hex), defaults to str\n" +
			"  offset: Offset within the existing to store the new data to. -1 to override existing data, defaults to 0";
}

// ugly way to support overloading and type dispatching
function exec() {

    if (arguments.length > 0 && typeof arguments[0] === "string") {
        exec_cid.apply(this, [dxram.longStrToLong(arguments[0])].concat(Array.prototype.slice.call(arguments, 1)));
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

function exec_cid(cid, data, offset, type) {

    if (cid == null) {
        dxterm.printlnErr("No cid specified");
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

    // don't allow put of index chunk
    if (dxram.nidOfCid(cid) == 0) {
        dxterm.printlnErr("Put of index chunk is not allowed.");
        return;
    }

    var chunkService = dxram.service("chunk");
    var chunks = chunkService.get(cid);

    if (chunks.first() == 0) {
        dxmterm.printlnErr("Getting chunk " + dxram.longToHexStr(cid) + " failed.");
        return;
    }

    var chunk = chunks.second()[0];
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
            dxterm.printlnErr("Unsupported data type " + type);
            return;
    }

    // put chunk back
    if (chunkService.put(chunk) != 1) {
        dxterm.printlnErr("Putting chunk " + dxram.longToHexStr(cid) + " failed");
    } else {
        dxterm.println("Put to chunk " + dxram.longToHexStr(cid) + " successful.");
    }
}
