
package de.hhu.bsinfo.dxgraph.load;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public abstract class AbstractGraphLoaderTaskPayload extends AbstractTaskPayload implements GraphLoaderResultDelegate {

	public static final String MS_DEFAULT_PATH = "DEFAULT";

	private String m_path = new String("");

	protected LoggerService m_loggerService;
	protected BootService m_bootService;
	protected ChunkService m_chunkService;

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

		m_loggerService.info(getClass(), "Loading graph, path '" + m_path + "' on slave id '" + getSlaveId() + "'...");
		int ret = load(m_path);
		if (ret != 0) {
			m_loggerService.info(getClass(), "Loading graph, path '" + m_path + "' successful.");
		} else {
			m_loggerService.error(getClass(), "Loading graph, path '" + m_path + "' failed.");
		}

		return ret;
	}

	protected abstract int load(final String p_path);
}
