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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Put chunk data to the temporary storage
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdTmpput extends AbstractTerminalCommand {
    public TcmdTmpput() {
        super("tmpput");
    }

    @Override
    public String getHelp() {
        return "Put chunk data to the temporary storage." + "If no offset is specified, the whole chunk is overwritten with the new data. " +
                "Otherwise the data is inserted at the starting offset with its length. " + "If the specified data is too long it will be truncated\n" +
                "Usage: tmpput <id> <data> [type] [offset] [length])\n" + "  id: Id of the chunk stored in temporary storage\n" +
                "  data: Data to store (format has to match type parameter)\n" +
                "  type: Type of the data to store (str, byte, short, int, long, hex), defaults to str\n" +
                "  offset: Offset within the existing to store the new data to. -1 to override existing data, defaults to 0";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        int id = p_cmd.getArgInt(0, -1);
        String data = p_cmd.getArgString(1, null);
        String type = p_cmd.getArgString(2, "str").toLowerCase();
        int offset = p_cmd.getArgInt(3, -1);

        if (id == -1) {
            p_stdout.printlnErr("No id specified");
            return;
        }

        if (data == null) {
            p_stdout.printlnErr("No data specified");
            return;
        }

        TemporaryStorageService tmp = p_services.getService(TemporaryStorageService.class);

        ChunkAnon chunk = new ChunkAnon(id);
        if (!tmp.getAnon(chunk)) {
            p_stdout.printflnErr("Getting chunk 0x%X failed: %s", id, chunk.getState());
            return;
        }

        if (offset == -1) {
            // wipe chunk
            for (int i = 0; i < chunk.getDataSize(); i++) {
                chunk.getData()[i] = 0;
            }
            offset = 0;
        }

        if (offset > chunk.sizeofObject()) {
            offset = chunk.sizeofObject();
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.getData());
        byteBuffer.position(offset);

        switch (type) {
            case "str":
                byte[] bytes = data.getBytes(java.nio.charset.StandardCharsets.US_ASCII);

                try {
                    int size = byteBuffer.capacity() - byteBuffer.position();
                    if (bytes.length < size) {
                        size = bytes.length;
                    }
                    byteBuffer.put(bytes, 0, size);
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "byte":
                try {
                    byteBuffer.put((byte) (Integer.parseInt(data) & 0xFF));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "short":
                try {
                    byteBuffer.putShort((short) (Integer.parseInt(data) & 0xFFFF));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "int":
                try {
                    byteBuffer.putInt(Integer.parseInt(data));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "long":
                try {
                    byteBuffer.putLong(Long.parseLong(data));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "hex":
                String[] tokens = data.split(" ");

                for (String str : tokens) {
                    try {
                        byteBuffer.put((byte) Integer.parseInt(str, 16));
                    } catch (final Exception ignored) {
                        // that's fine, trunc data
                    }
                }

                break;

            default:
                p_stdout.printflnErr("Unsupported data type %s", type);
                return;
        }

        // put chunk back
        if (!tmp.putAnon(chunk)) {
            p_stdout.printflnErr("Putting chunk 0x%X to tmp storage failed: %s", id, chunk.getState());
        } else {
            p_stdout.printfln("Put to chunk 0x%X to tmp storage successful", id);
        }
    }
}
