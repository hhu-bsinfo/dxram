package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.main.Main;
import de.hhu.bsinfo.utils.main.MainArguments;

public abstract class DXRAMMain extends Main {

	public static final Pair<String, String> ARG_DXRAM_CONF = new Pair<String, String>("dxramConfig", "config/dxram.conf");
	
	private DXRAM m_dxram = null;
	private String m_ip = null;
	private String m_port = null;
	private NodeRole m_role = null;
	
	public DXRAMMain()
	{
		m_dxram = new DXRAM();
	}
	
	public DXRAMMain(final String p_ip, final String p_port, final NodeRole p_role)
	{
		m_role = p_role;
		m_dxram = new DXRAM();
	}
	
	@Override
	protected void registerDefaultProgramArguments(MainArguments p_arguments) {
		p_arguments.setArgument(ARG_DXRAM_CONF);
	}
	
	@Override
	protected int main(MainArguments p_arguments) {
		if (!m_dxram.initialize(p_arguments.getArgument(ARG_DXRAM_CONF), m_ip, m_port, m_role, true))
		{
			System.out.println("Initializing DXRAM with configuration '" + p_arguments.getArgument(ARG_DXRAM_CONF) + "' failed.");
			return -1;
		}
		
		return mainApplication(p_arguments);
	}
	
	protected abstract int mainApplication(final MainArguments p_arguments);
	
	protected <T extends DXRAMService> T getService(final Class<T> p_class)
	{
		return m_dxram.getService(p_class);
	}
}
