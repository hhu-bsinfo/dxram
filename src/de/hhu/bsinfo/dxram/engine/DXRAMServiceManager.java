package de.hhu.bsinfo.dxram.engine;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.AsyncChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkMemoryService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.lock.PeerLockService;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.recovery.RecoveryService;
import de.hhu.bsinfo.dxram.script.ScriptEngineService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;

/**
 * Manager for all services in DXRAM.
 * All services used in DXRAM must be registered here to create a default configuration with all services listed.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 21.10.16
 */
public class DXRAMServiceManager {

	private static Map<String, Class<? extends AbstractDXRAMService>> m_registeredServices = new HashMap<>();

	/**
	 * Static class
	 */
	private DXRAMServiceManager() {

	}

	/**
	 * Register a service
	 *
	 * @param p_class Serivce class to register
	 */
	public static void register(final Class<? extends AbstractDXRAMService> p_class) {
		m_registeredServices.put(p_class.getSimpleName(), p_class);
	}

	/**
	 * Register all DXRAM services
	 */
	static void registerDefault() {
		register(AsyncChunkService.class);
		register(BootService.class);
		register(ChunkMemoryService.class);
		register(ChunkService.class);
		register(LogService.class);
		register(LoggerService.class);
		register(LookupService.class);
		register(MigrationService.class);
		register(NameserviceService.class);
		register(NetworkService.class);
		register(NullService.class);
		register(PeerLockService.class);
		register(RecoveryService.class);
		register(ScriptEngineService.class);
		register(SynchronizationService.class);
		register(TerminalService.class);
		register(TemporaryStorageService.class);
	}

	/**
	 * Create an instance of a service
	 *
	 * @param p_className Name of the class (without package path)
	 * @return Instance of the service
	 */
	static AbstractDXRAMService createInstance(final String p_className) {

		Class<? extends AbstractDXRAMService> clazz = m_registeredServices.get(p_className);

		try {
			return clazz.getConstructor().newInstance();
		} catch (final Exception e) {
			throw new RuntimeException("Cannot create service instance of " + clazz.getSimpleName(), e);
		}
	}

	/**
	 * Create instances of all registered services
	 *
	 * @return List of instances of all registered services
	 */
	static AbstractDXRAMService[] createAllInstances() {
		AbstractDXRAMService[] instances = new AbstractDXRAMService[m_registeredServices.size()];
		int index = 0;

		for (Class<? extends AbstractDXRAMService> clazz : m_registeredServices.values()) {
			try {
				instances[index++] = clazz.getConstructor().newInstance();
			} catch (final Exception e) {
				throw new RuntimeException("Cannot create component instance of " + clazz.getSimpleName(), e);
			}
		}

		return instances;
	}
}
