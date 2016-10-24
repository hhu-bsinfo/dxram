
package de.hhu.bsinfo.dxram.engine;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.utils.logger.Logger;

/**
 * Base class for all components in DXRAM. A component serves the engine as a building block
 * providing features and functions for a specific task. Splitting tasks/concepts/functions
 * across multiple components allows introducing a clearer structure and higher flexibility
 * for the whole system. Components are allowed to depend on other components i.e. directly
 * interact with each other.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public abstract class AbstractDXRAMComponent {

	// config values
	@Expose
	private final String m_class = this.getClass().getName();
	@Expose
	private final boolean m_enabled = true;
	@Expose
	private final short m_priorityInit;
	@Expose
	private final short m_priorityShutdown;

	private DXRAMEngine m_parentEngine;

	/**
	 * Constructor
	 *
	 * @param p_priorityInit     Default init priority for this component
	 * @param p_priorityShutdown Default shutdown priority for this component
	 */
	public AbstractDXRAMComponent(final short p_priorityInit, final short p_priorityShutdown) {
		m_priorityInit = p_priorityInit;
		m_priorityShutdown = p_priorityShutdown;
	}

	/**
	 * Get the name of this component.
	 *
	 * @return Name of this component.
	 */
	public String getComponentName() {
		return this.getClass().getSimpleName();
	}

	/**
	 * Get the init priority.
	 *
	 * @return Init priority.
	 */
	public int getPriorityInit() {
		return m_priorityInit;
	}

	/**
	 * Get the shutdown priority.
	 *
	 * @return Shutdown priority.
	 */
	public int getPriorityShutdown() {
		return m_priorityShutdown;
	}

	/**
	 * Initialize this component.
	 *
	 * @param p_engine Engine this component is part of (parent).
	 * @return True if initializing was successful, false otherwise.
	 */
	public boolean init(final DXRAMEngine p_engine) {
		boolean ret;

		m_parentEngine = p_engine;

		// #if LOGGER >= INFO
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Initializing component...");
		// #endif /* LOGGER >= INFO */

		try {
			ret = initComponent(m_parentEngine.getSettings());
		} catch (final Exception e) {
			// #if LOGGER >= ERROR
			m_parentEngine.getLogger().error(this.getClass().getSimpleName(), "Initializing component failed. ", e);
			// #endif /* LOGGER >= ERROR */
			return false;
		}

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
	 *
	 * @return True if shutting down was successful, false otherwise.
	 */
	public boolean shutdown() {
		boolean ret;

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
	 *
	 * @param <T>     Class of the implemented component.
	 * @param p_class Class of the component to get. If a component has multiple implementations, always use the base
	 *                class/interface here.
	 * @return Reference to the component requested or null if not available/enabled.
	 */
	protected <T extends AbstractDXRAMComponent> T getDependentComponent(final Class<T> p_class) {
		return m_parentEngine.getComponent(p_class);
	}

	/**
	 * Get the logger for logging messages.
	 *
	 * @return Logger for logging.
	 * @note There is a LoggerComponent class. The Logger is wrapped by this class to enable further
	 * control and features for logging messages. Use the LoggerComponent for logging instead.
	 */
	protected Logger getLogger() {
		return m_parentEngine.getLogger();
	}

	/**
	 * Get the parent engine of this component.
	 *
	 * @return Parent engine of this component.
	 * @note This is available for very special occasions. If you do not know what you are doing, don't use this method.
	 */
	protected DXRAMEngine getParentEngine() {
		return m_parentEngine;
	}

	/**
	 * Called when the component is initialized. Setup data structures, get dependent components, read settings etc.
	 *
	 * @param p_engineEngineSettings EngineSettings instance provided by the engine.
	 * @return True if initialing was successful, false otherwise.
	 */
	protected abstract boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings);

	/**
	 * Called when the component gets shut down. Cleanup your component in here.
	 *
	 * @return True if shutdown was successful, false otherwise.
	 */
	protected abstract boolean shutdownComponent();
}
