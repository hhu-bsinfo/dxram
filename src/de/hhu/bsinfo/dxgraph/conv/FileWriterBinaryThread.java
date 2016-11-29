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

package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Implementation of a writer to write vertex data to a binary file.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.02.2016
 */
class FileWriterBinaryThread extends AbstractFileWriterThread {
    /**
     * Constructor
     * @param p_outputPath
     *            Output file to write to.
     * @param p_id
     *            Id of the writer (0 based index).
     * @param p_idRangeStartIncl
     *            Range of vertex ids to write to the file, start.
     * @param p_idRangeEndIncl
     *            Range of the vertex ids to write the file, end.
     * @param p_storage
     *            Storage to access for vertex data to write to the file.
     */
    FileWriterBinaryThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl, final long p_idRangeEndIncl, final VertexStorage p_storage) {
        super(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndIncl, p_storage);
    }

    @Override
    public void run() {
        try {
            File file = new File(m_outputPath + "out.boel." + m_id);
            if (file.exists()) {
                if (!file.delete()) {
                    System.out.println("Deleting file " + file + " failed.");
                    m_errorCode = -1;
                }
            }

            File fileInfo = new File(m_outputPath + "out.ioel." + m_id);
            if (fileInfo.exists()) {
                if (!fileInfo.delete()) {
                    System.out.println("Deleting file " + file + " failed.");
                    m_errorCode = -2;
                }
            }

            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            BufferedWriter out2 = new BufferedWriter(new FileWriter(fileInfo));
            if (!dumpOrdered(out, out2, m_idRangeStartIncl, m_idRangeEndIncl)) {
                System.out.println("Dumping from vertex storage [" + m_idRangeStartIncl + ", " + m_idRangeEndIncl + "] failed.");
                out.close();
                out2.close();
            }

            out.close();
            out2.close();
        } catch (final IOException e) {
            System.out.println("Dumping to out file failed: " + e.getMessage());
            m_errorCode = -3;
            return;
        }

        System.out.println("Dumping [" + m_idRangeStartIncl + ", " + m_idRangeEndIncl + "] to file done");
        m_errorCode = 0;
    }

    /**
     * Write the vertex data to the file in ascending vertex id order. Also creates info file with metadata.
     * @param p_file
     *            File to write the vertex data to.
     * @param p_infoFile
     *            Info file with metadata.
     * @param p_rangeStartIncl
     *            VertexSimple id range start to write.
     * @param p_rangeEndIncl
     *            VertexSimple id range end to write.
     * @return True if successful, false on error.
     */
    private boolean dumpOrdered(final DataOutputStream p_file, final BufferedWriter p_infoFile, final long p_rangeStartIncl, final long p_rangeEndIncl) {
        long edgeCount = 0;
        long vertexCount = 0;
        long[] buffer = new long[10];
        for (long i = p_rangeStartIncl; i <= p_rangeEndIncl; i++) {
            long res = m_storage.getNeighbours(i, buffer);
            if (res < 0) {
                // buffer to small, enlarage and retry
                buffer = new long[(int) -res];
                i--;
                continue;
            } else if (res == Long.MAX_VALUE) {
                // System.out.println("Invalid vertex id entry discovered in storage: " + i);
                // write empty vertex
                try {
                    p_file.writeInt(0);
                } catch (IOException e) {
                    return false;
                }
            } else {

                try {
                    p_file.writeInt((int) res);
                    for (int j = 0; j < (int) res; j++) {
                        p_file.writeLong(buffer[j]);
                    }
                } catch (final IOException e) {
                    return false;
                }

                edgeCount += (int) res;
            }

            vertexCount++;
            updateProgress("TotalVerticesToFiles " + m_id, vertexCount, p_rangeEndIncl - p_rangeStartIncl);
        }

        try {
            p_infoFile.write(m_id + "," + Long.toString(vertexCount) + "," + Long.toString(edgeCount));
            p_file.flush();
            p_infoFile.flush();
        } catch (final IOException ignored) {
        }

        return true;
    }

}
