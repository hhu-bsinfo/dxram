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

package de.hhu.bsinfo.dxgraph.load;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraph.data.GraphPartitionIndex;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Load a partition index of a partitioned graph for one compute group. The index is
 * used to identify/convert single vertices or ranges of a partitioned graph on loading
 * the graph data.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GraphLoadPartitionIndexTask implements Task {
    public static final String MS_PART_INDEX_IDENT = "GPI";

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoadPartitionIndexTask.class.getSimpleName());

    @Expose
    private String m_pathFile = "./";

    private LoggerService m_loggerService;

    /**
     * Default constructor
     */
    public GraphLoadPartitionIndexTask() {

    }

    /**
     * Constructor
     *
     * @param p_pathFile
     *         Partition index file to load
     */
    public GraphLoadPartitionIndexTask(final String p_pathFile) {
        m_pathFile = p_pathFile;
    }

    /**
     * Set the path of the partition index file to load.
     *
     * @param p_path
     *         Path of the file to load.
     */
    public void setPartitionIndexFilePath(final String p_path) {
        m_pathFile = p_path;

        // trim / at the end
        if (m_pathFile.charAt(m_pathFile.length() - 1) == '/') {
            m_pathFile = m_pathFile.substring(0, m_pathFile.length() - 1);
        }
    }

    @Override
    public int execute(final TaskContext p_ctx) {

        // we don't have to execute this on all slaves
        // slave 0 will do this for the whole compute group and
        // store the result. every other slave can simply grab the
        // index from chunk memory
        if (p_ctx.getCtxData().getSlaveId() == 0) {
            m_loggerService = p_ctx.getDXRAMServiceAccessor().getService(LoggerService.class);
            TemporaryStorageService tmpStorage = p_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
            NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

            GraphPartitionIndex graphPartIndex = loadGraphPartitionIndexFromIndexFiles(p_ctx, m_pathFile);
            if (graphPartIndex == null) {
                return -1;
            }

            graphPartIndex.setID(tmpStorage.generateStorageId(MS_PART_INDEX_IDENT + p_ctx.getCtxData().getComputeGroupId()));

            // store the index for our current cLompute group
            if (!tmpStorage.create(graphPartIndex)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Creating chunk for partition index failed");
                // #endif /* LOGGER >= ERROR */
                return -2;
            }

            if (!tmpStorage.put(graphPartIndex)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Putting partition index failed");
                // #endif /* LOGGER >= ERROR */
                return -3;
            }

            // register chunk at nameservice that other slaves can find it
            nameserviceService.register(graphPartIndex, MS_PART_INDEX_IDENT + p_ctx.getCtxData().getComputeGroupId());

            // #if LOGGER >= INFO
            LOGGER.info("Successfully loaded and stored graph partition index, nameservice entry name %s:\n%s",
                    MS_PART_INDEX_IDENT + p_ctx.getCtxData().getComputeGroupId(), graphPartIndex);
            // #endif /* LOGGER >= INFO */
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        switch (p_signal) {
            case SIGNAL_ABORT: {
                // ignore signal here
                break;
            }
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_pathFile);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_pathFile = p_importer.readString(m_pathFile);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + m_pathFile.length();
    }

    /**
     * Load the graph partition index from one or multiple graph partition index files from a specific path.
     *
     * @param p_ctx
     *         TaskScript context.
     * @param p_path
     *         Path containing the graph partition index file(s).
     * @return Graph partition index object with partition entries loaded from the files.
     */
    private GraphPartitionIndex loadGraphPartitionIndexFromIndexFiles(final TaskContext p_ctx, final String p_path) {
        GraphPartitionIndex index = new GraphPartitionIndex();

        ArrayList<GraphPartitionIndex.Entry> entries = readIndexEntriesFromFile(p_ctx, p_path);
        if (entries != null) {
            for (GraphPartitionIndex.Entry entry : entries) {
                index.setPartitionEntry(entry);
            }
        } else {
            return null;
        }

        return index;
    }

    /**
     * Read the graph partition index from a single partition index file. The file can contain multiple entries (one per
     * line)
     *
     * @param p_ctx
     *         TaskScript context.
     * @param p_pathFile
     *         Path + filename of the index file to read.
     * @return List of entries read from the file or null on error.
     */
    private ArrayList<GraphPartitionIndex.Entry> readIndexEntriesFromFile(final TaskContext p_ctx, final String p_pathFile) {
        ArrayList<GraphPartitionIndex.Entry> entries = new ArrayList<>();
        short[] slaves = p_ctx.getCtxData().getSlaveNodeIds();

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(p_pathFile));
        } catch (final FileNotFoundException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Missing index file %s to create graph index for loading graph", p_pathFile);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // read all index entries of format <partition id>,<vertex count>,<edge count>
        while (true) {
            String line;
            try {
                line = reader.readLine();
            } catch (final IOException e) {
                break;
            }

            // eof
            if (line == null) {
                break;
            }

            String[] tokens = line.split(",");
            if (tokens.length != 4) {
                // #if LOGGER >= ERROR
                LOGGER.error("Invalid index entry %d in file %s, ignoring", line, p_pathFile);
                // #endif /* LOGGER >= ERROR */
                continue;
            }

            int partitionId = Integer.parseInt(tokens[0]);
            GraphPartitionIndex.Entry entry =
                    new GraphPartitionIndex.Entry(slaves[partitionId], partitionId, Long.parseLong(tokens[1]), Long.parseLong(tokens[2]),
                            Long.parseLong(tokens[3]));
            entries.add(entry);
        }

        try {
            reader.close();
        } catch (final IOException ignored) {
        }

        return entries;
    }
}
