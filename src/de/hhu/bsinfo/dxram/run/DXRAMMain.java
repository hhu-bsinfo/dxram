package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Base class for an entry point of a DXRAM application.
 * If DXRAM is integrated into an existing application,
 * just use the DXRAM class instead.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.02.16
 *
 */
public abstract class DXRAMMain extends Main {

	public static final Argument ARG_DXRAM_CONF = new Argument("dxramConfig", "config/dxram.conf", true, "Configuration file for DXRAM");
	
	private DXRAM m_dxram = null;
	private String m_ip = null;
	private String m_port = null;
	private NodeRole m_role = null;
	
	/**
	 * Default constructor
	 */
	public DXRAMMain()
	{
		super("DXRAM main entry point.");
		m_dxram = new DXRAM();
	}
	
	/**
	 * Constructor
	 * @param p_ip Ip address overriding the configuration value.
	 * @param p_port Port overriding the configuration value.
	 * @param p_role Node role overriding the configuration value.
	 */
	public DXRAMMain(final String p_ip, final String p_port, final NodeRole p_role)
	{
		super("DXRAM main entry point.");
		m_role = p_role;
		m_dxram = new DXRAM();
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_DXRAM_CONF);
	}
	
	@Override
	protected int main(ArgumentList p_arguments) {
		if (!m_dxram.initialize(p_arguments.getArgument(ARG_DXRAM_CONF).getValue(String.class), m_ip, m_port, m_role, true))
		{
			System.out.println("Initializing DXRAM with configuration '" + p_arguments.getArgument(ARG_DXRAM_CONF).getValue(String.class) + "' failed.");
			return -1;
		}
		
		return mainApplication(p_arguments);
	}
	
	/**
	 * Override this to implement your application built on top of DXRAM.
	 * @param p_arguments Arguments provided by the application.
	 * @return Exit code of the application.
	 */
	protected abstract int mainApplication(final ArgumentList p_arguments);
	
	/**
	 * Get a service from DXRAM.
	 * @param p_class Class of the service to get.
	 * @return DXRAM service or null if not available.
	 */
	protected <T extends DXRAMService> T getService(final Class<T> p_class)
	{
		return m_dxram.getService(p_class);
	}
	
	/**
	 * Get the DXRAM instance.
	 * @return DXRAM instance.
	 */
	protected DXRAM getDXRAM()
	{
		return m_dxram;
	}
}
