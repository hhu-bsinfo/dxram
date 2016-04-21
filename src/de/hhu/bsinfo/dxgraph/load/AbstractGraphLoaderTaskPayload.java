
package de.hhu.bsinfo.dxgraph.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public abstract class AbstractGraphLoaderTaskPayload extends AbstractTaskPayload {

	private String m_path = new String("");

	protected LoggerService m_loggerService;
	protected BootService m_bootService;
	protected ChunkService m_chunkService;

	protected GraphPartitionIndex m_graphPartitionIndex;

	public AbstractGraphLoaderTaskPayload(short p_typeId, short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

	public void setLoadPath(final String p_path) {
		m_path = p_path;
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_bootService = p_dxram.getService(BootService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);

		// normalize path
		int lastIndexPath = m_path.lastIndexOf('/');
		if (lastIndexPath != -1) {
			m_path = m_path.substring(lastIndexPath + 1);
		}

		// load graph partition index
		m_loggerService.info(getClass(), "Loading graph partition index, path '" + m_path + "'...");
		m_graphPartitionIndex = loadGraphPartitionIndexFromIndexFiles(m_path);
		m_loggerService.debug(getClass(), "Graph partition index loaded:\n" + m_graphPartitionIndex);

		m_loggerService.info(getClass(), "Loading graph, path '" + m_path + "' on slave id '" + getSlaveId() + "'...");
		int ret = loadGraphData(m_path);
		if (ret != 0) {
			m_loggerService.info(getClass(), "Loading graph, path '" + m_path + "' successful.");
		} else {
			m_loggerService.error(getClass(), "Loading graph, path '" + m_path + "' failed.");
		}

		return ret;
	}

	protected abstract int loadGraphData(final String p_path);

	/**
	 * Load the graph partition index from one or multiple graph partition index files from a specific path.
	 * @param p_path
	 *            Path containing the graph partition index file(s).
	 * @return Graph partition index object with partition entries loaded from the files.
	 */
	protected GraphPartitionIndex loadGraphPartitionIndexFromIndexFiles(final String p_path) {
		GraphPartitionIndex index = new GraphPartitionIndex();

		ArrayList<GraphPartitionIndex.Entry> entries = readIndexEntriesFromFiles(p_path);
		if (entries != null) {
			for (GraphPartitionIndex.Entry entry : entries) {
				index.setPartitionEntry(entry);
			}
		}

		return index;
	}

	/**
	 * Read graph partition index entries from one or multiple files from a specified folder.
	 * @param p_path
	 *            Path to the folder that contain the partition index files.
	 * @return List of partition index entries read from the partition index files or null on error.
	 */
	private ArrayList<GraphPartitionIndex.Entry> readIndexEntriesFromFiles(final String p_path) {
		ArrayList<GraphPartitionIndex.Entry> entries = new ArrayList<GraphPartitionIndex.Entry>();

		File dir = new File(p_path);
		if (!dir.exists()) {
			m_loggerService.error(getClass(), "Path " + p_path + " for graph partition index files does not exist.");
			return null;
		}

		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File p_dir, final String p_filename) {
				return p_filename.contains(".ioel.");
			}
		});

		m_loggerService.info(getClass(), "Found " + files.length + " graph partition index files in " + p_path);
		for (File file : files) {
			ArrayList<GraphPartitionIndex.Entry> tmp = readIndexEntriesFromFile(file.getAbsolutePath());
			if (tmp != null) {
				entries.addAll(tmp);
			}
		}

		return entries;
	}

	/**
	 * Read the graph partition index from a single partition index file. The file can contain multiple entries (one per
	 * line)
	 * @param p_pathFile
	 *            Path + filename of the index file to read.
	 * @return List of entries read from the file or null on error.
	 */
	private ArrayList<GraphPartitionIndex.Entry> readIndexEntriesFromFile(final String p_pathFile) {
		ArrayList<GraphPartitionIndex.Entry> entries = new ArrayList<GraphPartitionIndex.Entry>();
		short[] slaves = getSlaveNodeIds();

		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(p_pathFile));
		} catch (final FileNotFoundException e) {
			m_loggerService.error(getClass(),
					"Missing index file " + p_pathFile + " to create graph index for loading graph");
			return null;
		}

		// read all index entries of format <partition id>,<vertex count>,<edge count>
		while (true) {
			String line = null;
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
			if (tokens.length != 3) {
				m_loggerService.error(getClass(),
						"Invalid index entry " + line + " in file " + p_pathFile + ", ignoring.");
				continue;
			}

			int partitionId = Integer.parseInt(tokens[0]);
			GraphPartitionIndex.Entry entry = new GraphPartitionIndex.Entry(slaves[partitionId], partitionId,
					Long.parseLong(tokens[1]), Long.parseLong(tokens[1]));
			entries.add(entry);
		}

		try {
			reader.close();
		} catch (final IOException e) {}

		return entries;
	}
}
