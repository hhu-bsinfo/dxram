
package de.hhu.bsinfo.dxgraph.load2;

import de.hhu.bsinfo.dxram.data.ChunkID;

public class RebaseVertexIDLocal implements RebaseVertexID {

	private short m_curNodeId = -1;
	private long m_offset = -1;

	public RebaseVertexIDLocal(final short p_curNodeId, final long p_offset) {
		m_curNodeId = p_curNodeId;
		m_offset = p_offset;
	}

	@Override
	public long rebase(final long p_id) {
		return ChunkID.getChunkID(m_curNodeId, m_offset + p_id);
	}

	@Override
	public void rebase(final long[] p_ids) {
		for (int i = 0; i < p_ids.length; i++) {
			p_ids[i] = ChunkID.getChunkID(m_curNodeId, m_offset + p_ids[i]);
		}
	}
}