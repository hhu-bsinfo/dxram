/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxmonitor.state;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public abstract class AbstractState {

    /**
     * Abstract method. Has the function to update the state values.
     */
    public abstract void updateStats();

    /**
     * Reads the content of a whole file in a ByteBuffer. Returns the content as a String.
     *
     * @param p_path
     *     Path to File
     * @return file content
     */
    String readCompleteFile(final String p_path) {
        StringBuilder builder = new StringBuilder("");

        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(p_path);
        } catch (FileNotFoundException e) {
            return null;
        }
        FileChannel channel = fileInputStream.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
        byte[] barray = new byte[4096];
        int nRead;
        int nGet;
        try {
            while ((nRead = channel.read(buffer)) != -1) {
                if (nRead == 0) {
                    continue;
                }
                buffer.position(0);
                buffer.limit(nRead);
                while (buffer.hasRemaining()) {
                    nGet = Math.min(buffer.remaining(), 4096);
                    if (nGet != 4096) {
                        barray = new byte[nGet];
                    }
                    buffer.get(barray, 0, nGet);
                    builder.append(new String(barray));
                }
                buffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return builder.toString();
    }

}
