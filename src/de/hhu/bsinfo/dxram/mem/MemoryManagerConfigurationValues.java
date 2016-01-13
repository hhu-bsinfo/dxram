package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.utils.Pair;

public class MemoryManagerConfigurationValues {
	
	public static class Component {
		public static final Pair<String, Long> RAM_SIZE = new Pair<String, Long>("RamSize", 1024 * 1024 * 1024 * 4L);
		public static final Pair<String, Long> SEGMENT_SIZE = new Pair<String, Long>("SegmentSize", 1024 * 1024 * 1024 * 1L);
		public static final Pair<String, Boolean> STATISTICS = new Pair<String, Boolean>("Statistics", false);
	}
}
