package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Base class for an entry point of a DXRAM application.
 * If DXRAM is integrated into an existing application,
 * just use the DXRAM class instead.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.02.16
 *
 */
public class DXRAMMain extends Main {

	private DXRAM m_dxram = null;

	/**
	 * Main entry point
	 * @param args Program arguments.
	 */
	public static void main(final String[] args) {
		Main main = new DXRAMMain();
		main.run(args);
	}
	
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
	 * @param p_description Override the description for main.
	 */
	public DXRAMMain(final String p_description)
	{
		super(p_description);
		m_dxram = new DXRAM();
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		
	}
	
	@Override
	protected int main(ArgumentList p_arguments) {
		if (!m_dxram.initialize(true))
		{
			System.out.println("Initializing DXRAM failed.");
			return -1;
		}
		
		return mainApplication(p_arguments);
	}
	
	/**
	 * Override this to implement your application built on top of DXRAM.
	 * @param p_arguments Arguments provided by the application.
	 * @return Exit code of the application.
	 */
	protected int mainApplication(final ArgumentList p_arguments)
	{
		System.out.println("DXRAM started");
		
		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}
	
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
