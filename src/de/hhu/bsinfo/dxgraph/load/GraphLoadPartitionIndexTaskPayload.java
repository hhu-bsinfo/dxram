
package de.hhu.bsinfo.dxgraph.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Load a partition index of a partitioned graph for one compute group. The index is
 * used to identify/convert single vertices or ranges of a partitioned graph on loading
 * the graph data.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class GraphLoadPartitionIndexTaskPayload extends AbstractTaskPayload {
	public static final String MS_PART_INDEX_IDENT = "GPI";

	private static final Argument MS_ARG_PATH =
			new Argument("graphPath", null, false, "Path containing the graph index to load.");

	private LoggerService m_loggerService;

	private String m_path = "./";

	/**
	 * Constructor
	 */
	public GraphLoadPartitionIndexTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_PART_INDEX);
	}

	/**
	 * Set the path where one or multiple partition index files are stored.
	 *
	 * @param p_path Path where the files are located
	 */
	public void setLoadPath(final String p_path) {
		m_path = p_path;

		// trim / at the end
		if (m_path.charAt(m_path.length() - 1) == '/') {
			m_path = m_path.substring(0, m_path.length() - 1);
		}
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {

		// we don't have to execute this on all slaves
		// slave 0 will do this for the whole compute group and
		// store the result. every other slave can simply grab the
		// index from chunk memory
		if (getSlaveId() == 0) {
			m_loggerService = p_dxram.getService(LoggerService.class);
			TemporaryStorageService tmpStorage = p_dxram.getService(TemporaryStorageService.class);
			NameserviceService nameserviceService = p_dxram.getService(NameserviceService.class);

			GraphPartitionIndex graphPartIndex = loadGraphPartitionIndexFromIndexFiles(m_path);
			if (graphPartIndex == null) {
				return -1;
			}

			graphPartIndex.setID(tmpStorage.generateStorageId(MS_PART_INDEX_IDENT + getComputeGroupId()));

			// store the index for our current compute group
			if (!tmpStorage.create(graphPartIndex)) {
				m_loggerService.error(getClass(), "Creating chunk for partition index failed.");
				return -2;
			}

			if (!tmpStorage.put(graphPartIndex)) {
				m_loggerService.error(getClass(), "Putting partition index failed.");
				return -3;
			}

			// register chunk at nameservice that other slaves can find it
			nameserviceService.register(graphPartIndex, MS_PART_INDEX_IDENT + getComputeGroupId());

			m_loggerService.info(getClass(),
					"Successfully loaded and stored graph partition index, nameservice entry name "
							+ MS_PART_INDEX_IDENT + getComputeGroupId() + ":\n" + graphPartIndex);
		}

		return 0;
	}

	@Override
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {
		p_argumentList.setArgument(MS_ARG_PATH);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		m_path = p_argumentList.getArgumentValue(MS_ARG_PATH, String.class);
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = super.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_path.length());
		p_exporter.writeBytes(m_path.getBytes(StandardCharsets.US_ASCII));

		return size + Integer.BYTES + m_path.length();
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		int size = super.importObject(p_importer, p_size);

		int strLength = p_importer.readInt();
		byte[] tmp = new byte[strLength];
		p_importer.readBytes(tmp);
		m_path = new String(tmp, StandardCharsets.US_ASCII);

		return size + Integer.BYTES + m_path.length();
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_path.length();
	}

	/**
	 * Load the graph partition index from one or multiple graph partition index files from a specific path.
	 *
	 * @param p_path Path containing the graph partition index file(s).
	 * @return Graph partition index object with partition entries loaded from the files.
	 */
	private GraphPartitionIndex loadGraphPartitionIndexFromIndexFiles(final String p_path) {
		GraphPartitionIndex index = new GraphPartitionIndex();

		ArrayList<GraphPartitionIndex.Entry> entries = readIndexEntriesFromFiles(p_path);
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
	 * Read graph partition index entries from one or multiple files from a specified folder.
	 *
	 * @param p_path Path to the folder that contain the partition index files.
	 * @return List of partition index entries read from the partition index files or null on error.
	 */
	private ArrayList<GraphPartitionIndex.Entry> readIndexEntriesFromFiles(final String p_path) {
		ArrayList<GraphPartitionIndex.Entry> entries = new ArrayList<>();

		File dir = new File(p_path);
		if (!dir.exists()) {
			m_loggerService.error(getClass(), "Path " + p_path + " for graph partition index files does not exist.");
			return null;
		}

		File[] files = dir.listFiles((p_dir, p_filename) -> {
			return p_filename.contains(".ioel.");
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
	 *
	 * @param p_pathFile Path + filename of the index file to read.
	 * @return List of entries read from the file or null on error.
	 */
	private ArrayList<GraphPartitionIndex.Entry> readIndexEntriesFromFile(final String p_pathFile) {
		ArrayList<GraphPartitionIndex.Entry> entries = new ArrayList<>();
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
			if (tokens.length != 3) {
				m_loggerService.error(getClass(),
						"Invalid index entry " + line + " in file " + p_pathFile + ", ignoring.");
				continue;
			}

			int partitionId = Integer.parseInt(tokens[0]);
			GraphPartitionIndex.Entry entry = new GraphPartitionIndex.Entry(slaves[partitionId], partitionId,
					Long.parseLong(tokens[1]), Long.parseLong(tokens[2]));
			entries.add(entry);
		}

		try {
			reader.close();
		} catch (final IOException ignored) {
		}

		return entries;
	}
}
