
package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the memory manager.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class MemoryManagerConfigurationValues {

	/**
	 * Configuration values for the component.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
	 */
	public static class Component {
		public static final Pair<String, Long> RAM_SIZE = new Pair<String, Long>("RamSize", 1024 * 1024 * 1024 * 4L);
		public static final Pair<String, Long> SEGMENT_SIZE =
				new Pair<String, Long>("SegmentSize", 1024 * 1024 * 1024 * 1L);
	}
}
