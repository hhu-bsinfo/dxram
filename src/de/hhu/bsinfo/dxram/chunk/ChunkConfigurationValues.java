package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the chunk service.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 15.03.16
 *
 */
public class ChunkConfigurationValues {
	public static class Service {
		public static final Pair<String, Boolean> REMOVE_PERFORMANCE_CRITICAL_SECTIONS = new Pair<String, Boolean>("RemovePerformanceCriticalSections", false);
	}
}
