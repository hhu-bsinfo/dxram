
package de.hhu.bsinfo.dxram.engine;

import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.log.Logger;

/**
 * Base class for all services in DXRAM. All services in DXRAM form the API for the user.
 * Furthermore, different services allow splitting functionality that can be turned on/off
 * for different applications, create a clearer structure and higher flexibility. Services
 * use components to implement their functionality. A service is not allowed to depend on
 * another service and services can not interact with each other.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 25.01.16
 */
public abstract class AbstractDXRAMService {

	private DXRAMEngine m_parentEngine;
	private Settings m_settings;

	/**
	 * Constructor
	 */
	public AbstractDXRAMService() {

	}

	/**
	 * Get the name of this service.
	 * @return Name of this service.
	 */
	public String getServiceName() {
		return this.getClass().getSimpleName();
	}

	public void preInit(final DXRAMEngine p_engine) {
		m_parentEngine = p_engine;

		m_settings = new Settings(m_parentEngine.getConfiguration(), m_parentEngine.getLogger(), getServiceName());
		registerDefaultSettingsService(m_settings);
	}

	/**
	 * Start this service.
	 * @param p_engine
	 *            Engine this service is part of (parent).
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean start(final DXRAMEngine p_engine) {
		boolean ret = false;

		m_parentEngine = p_engine;
		m_settings = new Settings(m_parentEngine.getConfiguration(), m_parentEngine.getLogger(), getServiceName());

		registerDefaultSettingsService(m_settings);

		// #if LOGGER >= INFO
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Starting service...");
		// #endif /* LOGGER >= INFO */
		ret = startService(m_parentEngine.getSettings(), m_settings);
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
	 * @return True if shutting down was successful, false otherwise.
	 */
	public boolean shutdown() {
		boolean ret = false;

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
	 * @param <T>
	 *            Class implementing DXRAMComponent
	 * @param p_class
	 *            Class of the component to get. If a component has multiple implementations, always use the base
	 *            class/interface here.
	 * @return Reference to the component requested or null if not available/enabled.
	 */
	protected <T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
		return m_parentEngine.getComponent(p_class);
	}

	/**
	 * Register default value for any settings this service can get from the configuration.
	 * @param p_settings
	 *            Settings instance to register default values at.
	 */
	protected abstract void registerDefaultSettingsService(final Settings p_settings);

	/**
	 * Called when the service is initialized. Setup data structures, get components for operation, read settings etc.
	 * @param p_engineSettings
	 *            Settings instance provided by the engine.
	 * @param p_settings
	 *            Settings instance with service related settings.
	 * @return True if initialing was successful, false otherwise.
	 */
	protected abstract boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings);

	/**
	 * Called when the service gets shut down. Cleanup your service in here.
	 * @return True if shutdown was successful, false otherwise.
	 */
	protected abstract boolean shutdownService();

	/**
	 * Check if this class is a service accessor i.e. breaking the rules of
	 * not knowing other services. Override this if this feature is used.
	 * @return True if accessor, false otherwise.
	 */
	protected boolean isServiceAccessor() {
		return false;
	}

	/**
	 * Check if this class is an engine accessor i.e. breaking the rules of
	 * not knowing the engine. Override this if this feature is used.
	 * @return True if accessor, false otherwise.
	 */
	protected boolean isEngineAccessor() {
		return false;
	}

	/**
	 * Get the proxy class to access other services.
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
	 * @return Returns the parent engine if allowed to do so (override isEngineAccessor), null otherwise.
	 */
	protected DXRAMEngine getParentEngine() {
		if (isEngineAccessor()) {
			return m_parentEngine;
		} else {
			return null;
		}
	}

	/**
	 * Convenience wrapper to get service related settings from a configuration.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
	 */
	public static class Settings {
		private Configuration m_configuration;
		private Logger m_logger;
		private String m_basePath = new String();

		/**
		 * Constructor
		 * @param p_configuration
		 *            Configuration to wrap which contains service settings.
		 * @param p_logger
		 *            Logger to use for logging messages.
		 * @param p_serviceIdentifier
		 *            Identifier of the service used for the configuration path.
		 */
		Settings(final Configuration p_configuration, final Logger p_logger, final String p_serviceIdentifier) {
			m_configuration = p_configuration;
			m_logger = p_logger;
			m_basePath = "/DXRAMEngine/ServiceSettings/" + p_serviceIdentifier + "/";
		}

		/**
		 * Set a default value for a specific configuration key.
		 * @param <T>
		 *            Type of the value.
		 * @param p_default
		 *            Pair of configuration key and default value to set for the specified key.
		 */
		public <T> void setDefaultValue(final Pair<String, T> p_default) {
			setDefaultValue(p_default.first(), p_default.second());
		}

		/**
		 * Set a default value for a specific configuration key.
		 * @param <T>
		 *            Type of the value.
		 * @param p_key
		 *            Key for the value.
		 * @param p_value
		 *            Value to set.
		 */
		public <T> void setDefaultValue(final String p_key, final T p_value) {
			if (m_configuration.addValue(m_basePath + p_key, p_value, false)) {
				// we added a default value => value was missing from configuration
				// #if LOGGER >= WARN
				m_logger.warn(this.getClass().getSimpleName(),
						"Settings value for '" + p_key + "' is missing in " + m_basePath + ", using default value "
								+ p_value);
				// #endif /* LOGGER >= WARN */
			}
		}

		/**
		 * Get a value from the configuration for the service.
		 * @param <T>
		 *            Type of the value.
		 * @param p_default
		 *            Pair of key and default value to get value for.
		 * @return Value associated with the provided key.
		 */
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default) {
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}

		/**
		 * Get a value from the configuration for the service.
		 * @param <T>
		 *            Type of the value.
		 * @param p_key
		 *            Key to get the value for.
		 * @param p_type
		 *            Type of the value to get.
		 * @return Value assicated with the provided key.
		 */
		public <T> T getValue(final String p_key, final Class<T> p_type) {
			return m_configuration.getValue(m_basePath + p_key, p_type);
		}
	}
}
