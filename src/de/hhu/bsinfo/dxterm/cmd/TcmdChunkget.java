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

package de.hhu.bsinfo.dxterm.cmd;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Get a chunk from a storage
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkget extends AbstractTerminalCommand {
    public TcmdChunkget() {
        super("chunkget");
    }

    @Override
    public String getHelp() {
        return "Get a chunk specified by either full cid or separated lid + nid from a storage\n" + "Usage (1): chunkget <cid> <className>\n" +
                "Usage (2): chunkget <cid> [type] [hex] [offset] [length]\n" + "Usage (3): chunkget <nid> <lid> <className>\n" +
                "Usage (4): chunkget <nid> <lid> [type] [hex] [offset] [length]\n" + "  cid: Full chunk ID of the chunk to get data from\n" +
                "  nid: Separate node id part of the chunk to get data from\n" +
                "  lid: (In combination with) separate local id part of the chunk to get data from\n" +
                "  className: Full name of a java class that implements DataStructure (with package path)\n" +
                "An instance is created, the data is stored in that instance and printed.\n " +
                "  type: Format to print the data (\"str\", \"byte\", \"short\", \"int\", \"long\"), defaults to \"byte\"\n" +
                "  hex: For some representations, print as hex instead of decimal, defaults to true\n" +
                "  offset: Offset within the chunk to start getting data from, defaults to 0\n" +
                "  length: Number of bytes of the chunk to print, defaults to size of chunk";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        long cid;
        String className = null;
        String type = "byte";
        boolean hex = true;
        int offset = 0;
        int length = 0;

        if (p_cmd.getArgc() < 1) {
            p_stdout.printlnErr("No cid specified");
            return;
        }

        if (p_cmd.isArgChunkID(0)) {
            cid = p_cmd.getArgChunkId(0, ChunkID.INVALID_ID);

            if (p_cmd.getArgc() > 1) {
                if (isType(p_cmd.getArgString(1, ""))) {
                    type = p_cmd.getArgString(1, "byte").toLowerCase();
                    hex = p_cmd.getArgBoolean(2, true);
                    offset = p_cmd.getArgInt(3, 0);
                    length = p_cmd.getArgInt(4, 0);
                } else {
                    className = p_cmd.getArgString(1, null);
                }
            }
        } else {
            short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
            long lid = p_cmd.getArgLocalId(1, ChunkID.INVALID_ID);

            if (lid == ChunkID.INVALID_ID) {
                p_stdout.printlnErr("No lid specified");
                return;
            }

            cid = ChunkID.getChunkID(nid, lid);

            if (p_cmd.getArgc() > 2) {
                if (isType(p_cmd.getArgString(2, ""))) {
                    type = p_cmd.getArgString(2, "byte").toLowerCase();
                    hex = p_cmd.getArgBoolean(3, true);
                    offset = p_cmd.getArgInt(4, 0);
                    length = p_cmd.getArgInt(5, 0);
                } else {
                    className = p_cmd.getArgString(2, null);
                }
            }
        }

        if (cid == ChunkID.INVALID_ID) {
            p_stdout.printlnErr("No cid specified");
            return;
        }

        if (className != null) {
            DataStructure dataStructure = newDataStructure(className, p_stdout);
            if (dataStructure == null) {
                p_stdout.printflnErr("Creating data structure of name '%s' failed", className);
                return;
            }

            dataStructure.setID(cid);

            ChunkService chunk = p_services.getService(ChunkService.class);

            if (chunk.get(dataStructure) != 1) {
                p_stdout.printflnErr("Getting data structure 0x%X failed: %s", cid, chunk.getStatus());
                return;
            }

            p_stdout.printfln("DataStructure %s (size %d):\n%s", className, dataStructure.sizeofObject(), dataStructure);
        } else {
            ChunkAnonService chunkAnon = p_services.getService(ChunkAnonService.class);

            ChunkAnon[] chunks = new ChunkAnon[1];
            if (chunkAnon.get(chunks, cid) != 1) {
                p_stdout.printflnErr("Getting chunk 0x%X failed: %s", cid, chunks[0].getState());
                return;
            }

            ChunkAnon chunk = chunks[0];

            if (length == 0 || length > chunk.getDataSize()) {
                length = chunk.getDataSize();
            }

            if (offset > length) {
                offset = length;
            }

            if (offset + length > chunk.getDataSize()) {
                length = chunk.getDataSize() - offset;
            }

            String str = "";
            ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.getData());
            byteBuffer.position(offset);

            switch (type) {
                case "str":
                    try {
                        str = new String(chunk.getData(), offset, length, "US-ASCII");
                    } catch (final UnsupportedEncodingException e) {
                        p_stdout.printflnErr("Error encoding string: %s", e.getMessage());
                        return;
                    }

                    break;

                case "byte":
                    for (int i = 0; i < length; i += Byte.BYTES) {
                        if (hex) {
                            str += Integer.toHexString(byteBuffer.get() & 0xFF) + ' ';
                        } else {
                            str += byteBuffer.get() + " ";
                        }
                    }
                    break;

                case "short":
                    for (int i = 0; i < length; i += java.lang.Short.BYTES) {
                        if (hex) {
                            str += Integer.toHexString(byteBuffer.getShort() & 0xFFFF) + ' ';
                        } else {
                            str += byteBuffer.getShort() + " ";
                        }
                    }
                    break;

                case "int":
                    for (int i = 0; i < length; i += java.lang.Integer.BYTES) {
                        if (hex) {
                            str += Integer.toHexString(byteBuffer.getInt()) + ' ';
                        } else {
                            str += byteBuffer.getInt() + " ";
                        }
                    }
                    break;

                case "long":
                    for (int i = 0; i < length; i += java.lang.Long.BYTES) {
                        if (hex) {
                            str += Long.toHexString(byteBuffer.getLong()) + ' ';
                        } else {
                            str += byteBuffer.getLong() + " ";
                        }
                    }
                    break;

                default:
                    p_stdout.printflnErr("Unsuported data type %s", type);
                    return;
            }

            p_stdout.printfln("Chunk data of 0x%X (chunksize %d): \n%s", cid, chunk.sizeofObject(), str);
        }
    }

    private static boolean isType(final String p_str) {
        switch (p_str) {
            case "str":
            case "byte":
            case "short":
            case "int":
            case "long":
                return true;
            default:
                return false;
        }
    }

    private static DataStructure newDataStructure(final String p_className, final TerminalServerStdout p_stdout) {
        Class<?> clazz;
        try {
            clazz = Class.forName(p_className);
        } catch (final ClassNotFoundException ignored) {
            p_stdout.printflnErr("Cannot find class with name %s", p_className);
            return null;
        }

        if (!DataStructure.class.isAssignableFrom(clazz)) {
            p_stdout.printflnErr("Class %s is not implementing the DataStructure interface", p_className);
            return null;
        }

        DataStructure dataStructure;
        try {
            dataStructure = (DataStructure) clazz.getConstructor().newInstance();
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            p_stdout.printflnErr("Creating instance of %s failed: %s", p_className, e.getMessage());
            return null;
        }

        return dataStructure;
    }
}
