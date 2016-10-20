
package de.hhu.bsinfo.dxram.engine;

import java.lang.reflect.Modifier;
import java.util.Map;

import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.logger.Logger;

/**
 * Base class for all components in DXRAM. A component serves the engine as a building block
 * providing features and functions for a specific task. Splitting tasks/concepts/functions
 * across multiple components allows introducing a clearer structure and higher flexibility
 * for the whole system. Components are allowed to depend on other components i.e. directly
 * interact with each other.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public abstract class AbstractDXRAMComponent {

	private DXRAMEngine m_parentEngine;
	private Settings m_settings;

	private int m_priorityInit;
	private int m_priorityShutdown;

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public AbstractDXRAMComponent(final int p_priorityInit, final int p_priorityShutdown) {
		m_priorityInit = p_priorityInit;
		m_priorityShutdown = p_priorityShutdown;
	}

	/**
	 * Get the name of this component.
	 * @return Name of this component.
	 */
	public String getComponentName() {
		return this.getClass().getSimpleName();
	}

	/**
	 * Get the init priority.
	 * @return Init priority.
	 */
	public int getPriorityInit() {
		return m_priorityInit;
	}

	/**
	 * Get the shutdown priority.
	 * @return Shutdown priority.
	 */
	public int getPriorityShutdown() {
		return m_priorityShutdown;
	}

	/**
	 *
	 */
	public void preInit(final DXRAMEngine p_engine) {

		m_parentEngine = p_engine;

		String componentInterfaceIdentifier = new String();

		// is superclass abstract and not DXRAMComponent -> interface

		if (Modifier.isAbstract(this.getClass().getSuperclass().getModifiers())
				&& !this.getClass().getSuperclass().equals(AbstractDXRAMComponent.class)) {

			componentInterfaceIdentifier = this.getClass().getSuperclass().getSimpleName();

		}
		m_settings = new Settings(m_parentEngine.getConfiguration(),
				m_parentEngine.getLogger(),
				componentInterfaceIdentifier,
				this.getClass().getSimpleName());

		registerDefaultSettingsComponent(m_settings);


	}



	/**
	 * Initialize this component.
	 * @param p_engine
	 *            Engine this component is part of (parent).
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean init(final DXRAMEngine p_engine) {
		boolean ret = false;

		m_parentEngine = p_engine;

		String componentInterfaceIdentifier = new String();
		// is superclass abstract and not DXRAMComponent -> interface
		if (Modifier.isAbstract(this.getClass().getSuperclass().getModifiers())
				&& !this.getClass().getSuperclass().equals(AbstractDXRAMComponent.class)) {
			componentInterfaceIdentifier = this.getClass().getSuperclass().getSimpleName();
		}

		m_settings = new Settings(m_parentEngine.getConfiguration(),
				m_parentEngine.getLogger(),
				componentInterfaceIdentifier,
				this.getClass().getSimpleName());

		registerDefaultSettingsComponent(m_settings);

		// #if LOGGER >= INFO
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Initializing component...");
		// #endif /* LOGGER >= INFO */

		ret = initComponent(m_parentEngine.getSettings(), m_settings);
		if (!ret) {
			// #if LOGGER >= ERROR
			m_parentEngine.getLogger().error(this.getClass().getSimpleName(), "Initializing component failed.");
			// #endif /* LOGGER >= ERROR */
		} else {
			// #if LOGGER >= INFO
			m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Initializing component successful.");
			// #endif /* LOGGER >= INFO */
		}

		return ret;
	}

	/**
	 * Shut down this component.
	 * @return True if shutting down was successful, false otherwise.
	 */
	public boolean shutdown() {
		boolean ret = false;

		// #if LOGGER >= INFO
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down component...");
		// #endif /* LOGGER >= INFO */
		ret = shutdownComponent();
		if (!ret) {
			// #if LOGGER >= WARN
			m_parentEngine.getLogger().warn(this.getClass().getSimpleName(), "Shutting down component failed.");
			// #endif /* LOGGER >= WARN */
		} else {
			// #if LOGGER >= INFO
			m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down component successful.");
			// #endif /* LOGGER >= INFO */
		}

		return ret;
	}

	// ------------------------------------------------------------------------------

	/**
	 * Get a dependent component from the engine. Use it in this component if you have
	 * to interact with other components.
	 * @param <T>
	 *            Class of the implemented component.
	 * @param p_class
	 *            Class of the component to get. If a component has multiple implementations, always use the base
	 *            class/interface here.
	 * @return Reference to the component requested or null if not available/enabled.
	 */
	protected <T extends AbstractDXRAMComponent> T getDependentComponent(final Class<T> p_class) {
		return m_parentEngine.getComponent(p_class);
	}

	/**
	 * Get the logger for logging messages.
	 * @note There is a LoggerComponent class. The Logger is wrapped by this class to enable further
	 *       control and features for logging messages. Use the LoggerComponent for logging instead.
	 * @return Logger for logging.
	 */
	protected Logger getLogger() {
		return m_parentEngine.getLogger();
	}

	/**
	 * Get the parent engine of this component.
	 * @note This is available for very special occasions. If you do not know what you are doing, don't use this method.
	 * @return Parent engine of this component.
	 */
	protected DXRAMEngine getParentEngine() {
		return m_parentEngine;
	}

	/**
	 * Register default value for any settings this component can get from the configuration.
	 * @param p_settings
	 *            Settings instance to register default values at.
	 */
	protected abstract void registerDefaultSettingsComponent(final Settings p_settings);

	/**
	 * Called when the component is initialized. Setup data structures, get dependent components, read settings etc.
	 * @param p_engineSettings
	 *            Settings instance provided by the engine.
	 * @param p_settings
	 *            Settings instance with component related settings.
	 * @return True if initialing was successful, false otherwise.
	 */
	protected abstract boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings);

	/**
	 * Called when the component gets shut down. Cleanup your component in here.
	 * @return True if shutdown was successful, false otherwise.
	 */
	protected abstract boolean shutdownComponent();

	/**
	 * Convenience wrapper to get component related settings from a configuration.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
	 */
	public static class Settings {
		private static final String CONFIG_ROOT = "/DXRAMEngine/ComponentSettings/";

		private Configuration m_configuration;
		private Logger m_logger;
		private String m_commonBasePath = new String();
		private String m_basePath = new String();

		/**
		 * Constructor
		 * @param p_configuration
		 *            Configuration to wrap which contains components settings.
		 * @param p_logger
		 *            Logger to use for logging messages.
		 * @param p_componentInterfaceIdentifier
		 *            Identifier/Name of the component interface
		 *            (if the component has multiple implementations based on an abstract class/interface) used
		 *            for the configuration path.
		 * @param p_componentImplementationIdentifier
		 *            Identifier of the component used for the configuration path.
		 */
		Settings(final Configuration p_configuration, final Logger p_logger,
				final String p_componentInterfaceIdentifier, final String p_componentImplementationIdentifier) {
			m_configuration = p_configuration;
			m_logger = p_logger;
			m_commonBasePath = CONFIG_ROOT;
			if (!p_componentInterfaceIdentifier.isEmpty()) {
				m_commonBasePath += p_componentInterfaceIdentifier + "/";
			}
			m_basePath = m_commonBasePath + p_componentImplementationIdentifier + "/";
		}

		/**
		 * Set a default value for a specific configuration key.
		 * @param <T>
		 *            Class of the value.
		 * @param p_default
		 *            Pair of configuration key and default value to set for the specified key.
		 */
		public <T> void setDefaultValue(final Pair<String, T> p_default) {
			setDefaultValue(p_default.first(), p_default.second());
		}

		/**
		 * Set a default value for a specific configuration key.
		 * @param <T>
		 *            Class of the value.
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
						"Settings value for '" + p_key + "' is missing in " + m_basePath + ", using default value '"
								+ p_value + "'.");
				// #endif /* LOGGER >= WARN */
			}
		}

		/**
		 * Get a value from the configuration for the component.
		 * @param <T>
		 *            Class of the value.
		 * @param p_default
		 *            Pair of key and default value to get value for.
		 * @return Value associated with the provided key.
		 */
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default) {
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}

		/**
		 * Get a value from the configuration for the component.
		 * @param <T>
		 *            Class of the value.
		 * @param p_key
		 *            Key to get the value for.
		 * @param p_type
		 *            Type of the value to get.
		 * @return Value assicated with the provided key.
		 */
		public <T> T getValue(final String p_key, final Class<T> p_type) {
			// try implementation specific path first, then common interface path
			T val = m_configuration.getValue(m_basePath + p_key, p_type);
			if (val == null) {
				val = m_configuration.getValue(m_commonBasePath + p_key, p_type);
			}

			return val;
		}

		/**
		 * Get a list of values with the same key (but different indices).
		 * @param <T>
		 *            Class of the value.
		 * @param p_key
		 *            Key of the values to get.
		 * @param p_type
		 *            Type of the values to get.
		 * @return Map with values and their indices.
		 */
		public <T> Map<Integer, T> getValues(final String p_key, final Class<T> p_type) {
			// try implementation specific path first, then common interface path
			Map<Integer, T> vals = m_configuration.getValues(m_basePath + p_key, p_type);
			if (vals == null || vals.isEmpty()) {
				vals = m_configuration.getValues(m_commonBasePath + p_key, p_type);
			}

			return vals;
		}
	}
}
