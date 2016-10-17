package de.hhu.bsinfo.dxram.script;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Created by nothaas on 10/14/16.
 */
public interface ScriptDXRAMContext {

	void list();

	AbstractDXRAMService service(final String p_serviceName);

	String nidhexstr(final short nodeId);

	String cidhexstr(final long chunkId);

	NodeRole noderole(final String p_str);

	void sleep(final int p_timeMs);

	long cid(final short p_nid, final long p_lid);

	Chunk newChunk();

	Chunk newChunk(final int p_bufferSize);

	Chunk newChunk(final ByteBuffer p_buffer);

	Chunk newChunk(final long p_id);

	Chunk newChunk(final long p_id, final int p_bufferSize);

	Chunk newChunk(final long p_id, final ByteBuffer p_buffer);

	DataStructure newDataStructure(final String p_className);
}
