package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Main class/entry point for any application to work with DXRAM and its services.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public final class DXRAM 
{
	private DXRAMEngine m_engine;
	
	/**
	 * Constructor
	 */
	public DXRAM()
	{
		m_engine = new DXRAMEngine();
	}
	
	/**
	 * Initialize the instance.
	 * @param p_configurationFile Relative or absolute path to the DXRAM configuration file.
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final String p_configurationFile) {
		return initialize(p_configurationFile, null, null, null);
	}
	
	/**
	 * Initialize the instance.
	 * @param p_configurationFile Relative or absolute path to the DXRAM configuration file.
	 * @param p_autoShutdown True to have DXRAM shut down automatically when the application quits. 
	 * 			If false, the caller has to take care of shutting down the instance by calling shutdown when done. 
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final String p_configurationFile, final boolean p_autoShutdown) {
		boolean ret = initialize(p_configurationFile, null, null, null);
		if (ret & p_autoShutdown)
			Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
		return ret;
	}
	
	/**
	 * Initialize the instance.
	 * @param p_configurationFile Relative or absolute path to the DXRAM configuration file.
	 * @param p_overrideIp Overriding the configuration file provided IP address (example: 127.0.0.1).
	 * @param p_overridePort Overriding the configuration file provided port number (example: 22223).
	 * @param p_overrideRole Overriding the configuration file provided role (example: Superpeer).
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final String p_configurationFile, final String p_overrideIp, 
			final String p_overridePort, final NodeRole p_overrideRole) {
		boolean ret = m_engine.init(p_configurationFile, p_overrideIp, p_overridePort, p_overrideRole);
		printNodeInfo();
		return ret;
	}
	
	/**
	 * Initialize the instance.
	 * @param p_configurationFile Relative or absolute path to the DXRAM configuration file.
	 * @param p_overrideIp Overriding the configuration file provided IP address (example: 127.0.0.1).
	 * @param p_overridePort Overriding the configuration file provided port number (example: 22223).
	 * @param p_overrideRole Overriding the configuration file provided role (example: Superpeer).
	 * @param p_autoShutdown True to have DXRAM shut down automatically when the application quits. 
	 * 			If false, the caller has to take care of shutting down the instance by calling shutdown when done. 
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean initialize(final String p_configurationFile, final String p_overrideIp, 
			final String p_overridePort, final NodeRole p_overrideRole, final boolean p_autoShutdown) {
		boolean ret = m_engine.init(p_configurationFile, p_overrideIp, p_overridePort, p_overrideRole);
		if (ret & p_autoShutdown)
			Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
		printNodeInfo();
		return ret;
	}
	
	/**
	 * Get a service from DXRAM.
	 * @param p_class Class of the service to get. If one service has multiple implementations, use
	 * 			the common super class here.
	 * @return Service requested or null if the service is not enabled/available.
	 */
	public <T extends DXRAMService> T getService(final Class<T> p_class) {		   
		return m_engine.getService(p_class);
	}

	/**
	 * Shut down DXRAM. Call this if you have not enabled auto shutdown on init.
	 */
	public void shutdown() {
		m_engine.shutdown();
	}
	
	/**
	 * Print some information after init about our current node.
	 */
	private void printNodeInfo()
	{
		BootService bootService = m_engine.getService(BootService.class);
		System.out.println(">>> DXRAM Node <<<");
		short nodeId = bootService.getNodeID();
		System.out.println("NodeID: " + Integer.toHexString(nodeId).substring(4) + " (" + nodeId + ")");
		System.out.println("Role: " + bootService.getNodeRole(nodeId));
	}
	
	/**
	 * Shuts down DXRAM in case of the system exits
	 * @author Florian Klein 03.09.2013
	 */
	private static final class ShutdownThread extends Thread {

		private DXRAM m_dxram = null;
		
		/**
		 * Creates an instance of ShutdownThread
		 * @param p_dxram Reference to DXRAM instance.
		 */
		private ShutdownThread(final DXRAM p_dxram) {
			super(ShutdownThread.class.getSimpleName());
			m_dxram = p_dxram;
		}

		@Override
		public void run() {
			m_dxram.shutdown();
		}

	}
}
