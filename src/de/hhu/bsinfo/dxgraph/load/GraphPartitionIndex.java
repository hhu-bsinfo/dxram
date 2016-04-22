
package de.hhu.bsinfo.dxgraph.load;

import java.util.Map;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Graph partition index for partitioned graph.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 21.04.16
 */
public class GraphPartitionIndex implements DataStructure {
	private long m_id = ChunkID.INVALID_ID;
	private Map<Integer, Entry> m_index = new TreeMap<Integer, Entry>();

	/**
	 * Constructor
	 */
	public GraphPartitionIndex() {

	}

	/**
	 * Set a partition entry for the index.
	 * @param p_entry
	 *            Entry to set/add.
	 */
	public void setPartitionEntry(final Entry p_entry) {
		m_index.put(p_entry.m_partitionIndex, p_entry);
	}

	/**
	 * Get a partition index entry from the index.
	 * @param p_partitionId
	 *            Id of the partition index entry to get.
	 * @return Partition index entry or null if there is no entry for the specified id.
	 */
	public Entry getPartitionIndex(final int p_partitionId) {
		return m_index.get(p_partitionId);
	}

	/**
	 * Rebase a graph global vertexId to a partition local vertex id using the index.
	 * @param p_vertexId
	 *            Graph global vertexId to rebase.
	 * @return Rebased vertex id to the partition the vertex is in.
	 */
	public long rebaseGlobalVertexIdToLocalPartitionVertexId(final long p_vertexId) {
		// find section the vertex (of the neighbor) is in
		long globalVertexIDOffset = 0;
		for (Entry entry : m_index.values()) {
			if (p_vertexId >= globalVertexIDOffset && p_vertexId <= globalVertexIDOffset + entry.m_vertexCount) {
				return ChunkID.getChunkID(entry.m_nodeId, p_vertexId - globalVertexIDOffset);
			}

			globalVertexIDOffset += entry.m_vertexCount;
		}

		// out of range ID
		return ChunkID.INVALID_ID;
	}

	/**
	 * Rebase multiple graph global vertexIds in plance to partition local vertex ids using the index.
	 * @param p_vertexIds
	 *            Graph global vertexIds to rebase.
	 */
	public void rebaseGlobalVertexIdToLocalPartitionVertexId(final long[] p_vertexIds) {
		// utilize locality instead of calling function
		for (int i = 0; i < p_vertexIds.length; i++) {
			// out of range ID, default assign if not found in loop
			long tmp = ChunkID.INVALID_ID;

			// find section the vertex (of the neighbor) is in
			long globalVertexIDOffset = 0;
			for (Entry entry : m_index.values()) {
				if (p_vertexIds[i] >= globalVertexIDOffset
						&& p_vertexIds[i] <= globalVertexIDOffset + entry.m_vertexCount) {
					tmp = ChunkID.getChunkID(entry.m_nodeId, p_vertexIds[i] - globalVertexIDOffset);
					break;
				}

				globalVertexIDOffset += entry.m_vertexCount;
			}

			p_vertexIds[i] = tmp;
		}
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		int size = p_importer.readInt();
		m_index = new TreeMap<Integer, Entry>();
		for (int i = 0; i < size; i++) {
			Entry entry = new Entry();
			p_importer.importObject(entry);
			m_index.put(entry.m_partitionIndex, entry);
		}

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		if (m_index.isEmpty()) {
			return Integer.BYTES;
		} else {
			return Integer.BYTES + m_index.size() * m_index.get(0).sizeofObject();
		}
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		p_exporter.writeInt(m_index.size());
		for (Entry entry : m_index.values()) {
			p_exporter.exportObject(entry);
		}

		return sizeofObject();
	}

	@Override
	public long getID() {
		return m_id;
	}

	@Override
	public void setID(final long p_id) {
		m_id = p_id;
	}

	@Override
	public String toString() {
		String str = new String();
		for (Entry entry : m_index.values()) {
			str += entry + "\n";
		}

		return str;
	}

	/**
	 * Single partition index entry.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 21.04.16
	 */
	public static class Entry implements DataStructure {
		private long m_id = ChunkID.INVALID_ID;

		private short m_nodeId = -1;
		private int m_partitionIndex = -1;
		private long m_vertexCount = -1;
		private long m_edgeCount = -1;

		/**
		 * Default constructor
		 */
		public Entry() {

		}

		/**
		 * Constructor
		 * @param p_nodeId
		 *            Node id the partition gets assigned to.
		 * @param p_partitionIndex
		 *            Partition index.
		 * @param p_vertexCount
		 *            Number of vertices in this partition.
		 * @param p_edgeCount
		 *            Number of edges in this partition.
		 */
		public Entry(final short p_nodeId, final int p_partitionIndex, final long p_vertexCount,
				final long p_edgeCount) {
			m_nodeId = p_nodeId;
			m_partitionIndex = p_partitionIndex;
			m_vertexCount = p_vertexCount;
			m_edgeCount = p_edgeCount;
		}

		/**
		 * Get the node id this partition is assigned to.
		 * @return Node Id.
		 */
		public short getNodeId() {
			return m_nodeId;
		}

		/**
		 * Get the partition id.
		 * @return Partition id.
		 */
		public int getPartitionId() {
			return m_partitionIndex;
		}

		/**
		 * Get the vertex count of the partition.
		 * @return Vertex count.
		 */
		public long getVertexCount() {
			return m_vertexCount;
		}

		/**
		 * Get the edge count of the partition.
		 * @return Edge count.
		 */
		public long getEdgeCount() {
			return m_edgeCount;
		}

		@Override
		public int importObject(final Importer p_importer, final int p_size) {
			m_nodeId = p_importer.readShort();
			m_partitionIndex = p_importer.readInt();
			m_vertexCount = p_importer.readLong();
			m_edgeCount = p_importer.readLong();

			return sizeofObject();
		}

		@Override
		public int sizeofObject() {
			return Short.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;
		}

		@Override
		public boolean hasDynamicObjectSize() {
			return false;
		}

		@Override
		public int exportObject(final Exporter p_exporter, final int p_size) {
			p_exporter.writeShort(m_nodeId);
			p_exporter.writeInt(m_partitionIndex);
			p_exporter.writeLong(m_vertexCount);
			p_exporter.writeLong(m_edgeCount);

			return sizeofObject();
		}

		@Override
		public long getID() {
			return m_id;
		}

		@Override
		public void setID(final long p_id) {
			m_id = p_id;
		}

		@Override
		public String toString() {
			return m_partitionIndex + ", " + NodeID.toHexString(m_nodeId) + ", " + m_vertexCount + ", " + m_edgeCount;
		}
	}
}
