
package de.hhu.bsinfo.dxram.engine;

import com.google.gson.annotations.Expose;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

	private final Logger LOGGER;

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
		LOGGER = LogManager.getFormatterLogger(this.getClass().getSimpleName());
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
		LOGGER.info("Initializing component...");
		// #endif /* LOGGER >= INFO */

		resolveComponentDependencies(p_engine);

		try {
			ret = initComponent(m_parentEngine.getSettings());
		} catch (final Exception e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Initializing component failed", e);
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (!ret) {
			// #if LOGGER >= ERROR
			LOGGER.error("Initializing component failed");
			// #endif /* LOGGER >= ERROR */
		} else {
			// #if LOGGER >= INFO
			LOGGER.info("Initializing component successful");
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
		LOGGER.info("Shutting down component...");
		// #endif /* LOGGER >= INFO */
		ret = shutdownComponent();
		if (!ret) {
			// #if LOGGER >= WARN
			LOGGER.warn("Shutting down component failed");
			// #endif /* LOGGER >= WARN */
		} else {
			// #if LOGGER >= INFO
			LOGGER.info("Shutting down component successful");
			// #endif /* LOGGER >= INFO */
		}

		return ret;
	}

	// ------------------------------------------------------------------------------

	/**
	 * Called before the component is initialized. Get all the components your own component depends on.
	 *
	 * @param p_componentAccessor Component accessor that provides access to other components
	 */
	protected abstract void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor);

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

	/**
	 * Check if this class is an engine accessor i.e. breaking the rules of
	 * not knowing the engine. Override this if this feature is used.
	 * Do not override this if you do not know what you are doing.
	 *
	 * @return True if accessor, false otherwise.
	 */
	protected boolean isEngineAccessor() {
		return false;
	}

	/**
	 * Get the engine within the component.
	 * If you don't know what you are doing, do not use this.
	 * There are some internal exceptions that make this necessary (like triggering a shutdown or reboot)
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
