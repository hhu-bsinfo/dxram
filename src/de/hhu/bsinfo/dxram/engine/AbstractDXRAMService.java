
package de.hhu.bsinfo.dxram.engine;

import com.google.gson.annotations.Expose;

/**
 * Base class for all services in DXRAM. All services in DXRAM form the API for the user.
 * Furthermore, different services allow splitting functionality that can be turned on/off
 * for different applications, create a clearer structure and higher flexibility. Services
 * use components to implement their functionality. A service is not allowed to depend on
 * another service and services can not interact with each other.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 25.01.16
 */
public abstract class AbstractDXRAMService {

	// config values
	@Expose
	private final String m_class = this.getClass().getName();
	@Expose
	private final boolean m_enabled = true;

	private String m_shortName;
	private DXRAMEngine m_parentEngine;

	/**
	 * Constructor
	 *
	 * @param p_shortName Short name of the service (used for terminal)
	 */
	public AbstractDXRAMService(final String p_shortName) {
		m_shortName = p_shortName;
	}

	/**
	 * Get the short name/identifier for this service.
	 *
	 * @return Identifier/name for this service.
	 */
	public String getShortName() {
		return m_shortName;
	}

	/**
	 * Get the name of this service.
	 *
	 * @return Name of this service.
	 */
	public String getServiceName() {
		return this.getClass().getSimpleName();
	}

	/**
	 * Start this service.
	 *
	 * @param p_engine Engine this service is part of (parent).
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean start(final DXRAMEngine p_engine) {
		boolean ret;

		m_parentEngine = p_engine;

		// #if LOGGER >= INFO
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Starting service...");
		// #endif /* LOGGER >= INFO */
		ret = startService(m_parentEngine.getSettings());
		if (!ret) {
			// #if LOGGER >= ERROR
			m_parentEngine.getLogger().error(this.getClass().getSimpleName(), "Starting service failed.");
			// #endif /* LOGGER >= ERROR */
		} else {
			// #if LOGGER >= INFO
			m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Starting service successful.");
			// #endif /* LOGGER >= INFO */
		}

		return ret;
	}

	/**
	 * Shut down this service.
	 *
	 * @return True if shutting down was successful, false otherwise.
	 */
	public boolean shutdown() {
		boolean ret;

		// #if LOGGER >= INFO
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down service...");
		// #endif /* LOGGER >= INFO */
		ret = shutdownService();
		if (!ret) {
			// #if LOGGER >= WARN
			m_parentEngine.getLogger().warn(this.getClass().getSimpleName(), "Shutting down service failed.");
			// #endif /* LOGGER >= WARN */
		} else {
			// #if LOGGER >= INFO
			m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down service successful.");
			// #endif /* LOGGER >= INFO */
		}

		return ret;
	}

	/**
	 * Get a component from the engine.
	 *
	 * @param <T>     Class implementing DXRAMComponent
	 * @param p_class Class of the component to get. If a component has multiple implementations, always use the base
	 *                class/interface here.
	 * @return Reference to the component requested or null if not available/enabled.
	 */
	protected <T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
		return m_parentEngine.getComponent(p_class);
	}

	/**
	 * Called when the service is initialized. Setup data structures, get components for operation, read settings etc.
	 *
	 * @param p_engineEngineSettings EngineSettings instance provided by the engine.
	 * @return True if initialing was successful, false otherwise.
	 */
	protected abstract boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings);

	/**
	 * Called when the service gets shut down. Cleanup your service in here.
	 *
	 * @return True if shutdown was successful, false otherwise.
	 */
	protected abstract boolean shutdownService();

	/**
	 * Check if this class is a service accessor i.e. breaking the rules of
	 * not knowing other services. Override this if this feature is used.
	 *
	 * @return True if accessor, false otherwise.
	 */
	protected boolean isServiceAccessor() {
		return false;
	}

	/**
	 * Check if this class is an engine accessor i.e. breaking the rules of
	 * not knowing the engine. Override this if this feature is used.
	 *
	 * @return True if accessor, false otherwise.
	 */
	protected boolean isEngineAccessor() {
		return false;
	}

	/**
	 * Get the proxy class to access other services.
	 *
	 * @return This returns a valid accessor only if the class is declared a service accessor.
	 */
	protected DXRAMServiceAccessor getServiceAccessor() {
		if (isServiceAccessor()) {
			return m_parentEngine;
		} else {
			return null;
		}
	}

	/**
	 * Get the engine within the service.
	 * This is not wanted in most cases to hide as much as possible from the services.
	 * But for some exceptions (like triggering a shutdown or reboot) there is no other way.
	 *
	 * @return Returns the parent engine if allowed to do so (override isEngineAccessor), null otherwise.
	 */
	protected DXRAMEngine getParentEngine() {
		if (isEngineAccessor()) {
			return m_parentEngine;
		} else {
			return null;
		}
	}
}
