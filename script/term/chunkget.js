function help() {
	return "Get a chunk specified by either full cid or separted lid + nid from a storage\n" +
			"Parameters: size nid\n" +
			"  size: Size of the chunk to create\n" +
			"  nid: Node id of the peer to create the chunk on"
}

// ugly work around for non supported overloading and type dispatching
function exec() {

    switch (arguments.length) {
        case 0:
            print("1")
            exec1()
            break
        case 1:
            print(2)
            exec1(arguments)
            break
        case 2:
            print(3)
            exec1(arguments)
            break
        default:
            print(4)
    }
}

function exec1(id1, id2) {

    if (id1 == null) {
        print("No cid or nid specified")
        return
    }

    if (id2 == null) {
        p_exec1(id1)
    } else {
        p_exec1(dxram.cid(id1, id2))
    }
}

function p_exec1(cid) {
    if (cid == null) {
        print("No cid specified")
        return
    }

    var chunk = dxram.service("chunk")

    var res = chunk.get(cid)
    if (res.first() == 0) {
        print("Getting chunk " + dxram.cidhexstr(cid) + " failed.")
        return
    }

    print("Chunk (" + dxram.cidhexstr(cid) + "): ")
}

//function printChunk(chunk, offset, dataType, hex) {
//
//    dataType = dataType.toLowerCase();
//
//    switch (dataType) {
//
//        case "str":
//            var bytes = new byte[buffer.capacity() - buffer.position()];
//
//            try {
//                buffer.get(bytes, 0, len);
//            } catch (e) {
//                // that's fine, trunc data
//            }
//
//            str = new String(bytes, StandardCharsets.US_ASCII);
//    }
//
//    if (dataType.equals("str")) {
//        byte[] bytes = new byte[buffer.capacity() - buffer.position()];
//
//        try {
//            buffer.get(bytes, 0, len);
//        } catch (final BufferOverflowException e) {
//            // that's fine, trunc data
//        }
//
//        str = new String(bytes, StandardCharsets.US_ASCII);
//    } else if (dataType.equals("byte")) {
//        try {
//            for (int i = 0; i < len; i += Byte.BYTES) {
//                if (hex) {
//                    str += Integer.toHexString(buffer.get() & 0xFF) + " ";
//                } else {
//                    str += (char) buffer.get();
//                }
//
//            }
//        } catch (final BufferOverflowException e) {
//            // that's fine, trunc data
//        }
//    } else if (dataType.equals("short")) {
//        try {
//            for (int i = 0; i < len; i += Short.BYTES) {
//                if (hex) {
//                    str += Integer.toHexString(buffer.getShort() & 0xFFFF) + " ";
//                } else {
//                    str += buffer.getShort() + " ";
//                }
//            }
//        } catch (final BufferOverflowException e) {
//            // that's fine, trunc data
//        }
//    } else if (dataType.equals("int")) {
//        try {
//            for (int i = 0; i < len; i += Integer.BYTES) {
//                if (hex) {
//                    str += Integer.toHexString(buffer.getInt() & 0xFFFFFFFF) + " ";
//                } else {
//                    str += buffer.getInt() + " ";
//                }
//            }
//        } catch (final BufferOverflowException e) {
//            // that's fine, trunc data
//        }
//    } else if (dataType.equals("long")) {
//        try {
//            for (int i = 0; i < len; i += Long.BYTES) {
//                if (hex) {
//                    str += Long.toHexString(buffer.getLong() & 0xFFFFFFFFFFFFFFFFL) + " ";
//                } else {
//                    str += buffer.getLong() + " ";
//                }
//            }
//        } catch (final BufferOverflowException e) {
//            // that's fine, trunc data
//        }
//    } else {
//        getTerminalDelegate().println("error: Unsupported data type " + dataType, TerminalColor.RED);
//        return true;
//    }
//}

//function exec2(nid, lid) {
//    if (nid == null) {
//        print("No nid specified")
//        return
//    }
//
//    if (lid == null) {
//        print("No lid specified")
//        return
//    }
//
//    exec1(dxram.cid(nid, lid))
//}
//
//function exec2_(cid, className) {
//    if (cid == null) {
//        print("No cid specified")
//        return
//    }
//
//    if (className == null) {
//        print("No class name specified")
//        return
//    }
//
//    var ds = dxram.newDataStructure(className);
//    if (ds == null) {
//        print("Creating DataStructure of class type '" + className + "' failed")
//        return
//    }
//
//    var chunk = dxram.service("chunk")
//
//    ds.setID(cid)
//    if (chunk.get(ds) != 1) {
//        print("Getting data structure " + dxram.cidhexstr(cid) + " failed.")
//        return
//    }
//
//    print("DataStructure " + className + ": " + ds)
//}
//
//function exec3(nid, lid, className) {
//    if (nid == null) {
//        print("No nid specified")
//        return
//    }
//
//    if (lid == null) {
//        print("No lid specified")
//        return
//    }
//
//    if (className == null) {
//        print("No class name specified")
//        return
//    }
//}
//
//function exec4(cid, offset, type, hex) {
//    if (cid == null) {
//        print("No cid specified")
//        return
//    }
//
//    if (offset == null) {
//        offset = 0
//    }
//
//    if (type == null) {
//        type = "byte"
//    }
//
//    if (hex == null) {
//        hex = true
//    }
//}
//
//function exec6(nid, lid, offset, type, hex, clazz) {
//    if (!nid) {
//        print("No nid specified")
//        return
//    }
//
//    if (!lid) {
//        print("No lid specified")
//        return
//    }
//}

//	if (!size) {
//		print("No size specified")
//		return
//	}
//
//	if (!nid) {
//		print("No nid specified")
//	}
//
//	var chunk = dxram.service("chunk")
//
//	var chunkIDs = chunk.createRemote(nid, size)
//
//	if (chunkIDs != null) {
//	    print("Created chunk of size " + size + ": " + dxram.cidhexstr(chunkIDs[0]))
//	} else {
//        print("Creating chunk failed.")
//	}
//}
