
package de.hhu.bsinfo.dxram.backup;

import de.hhu.bsinfo.utils.Pair;

/**
 * Configuration values for the backup service.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 15.03.16
 */
public class BackupConfigurationValues {

	/**
	 * Backup component configuration attributes
	 */
	public static class Component {
		public static final Pair<String, Boolean> BACKUP_ACTIVE = new Pair<String, Boolean>("BackupActive", false);
		public static final Pair<String, String> BACKUP_DIRECTORY = new Pair<String, String>("BackupDirectory", "./log/");
		public static final Pair<String, Long> BACKUP_RANGE_SIZE = new Pair<String, Long>("BackupRangeSize", 265 * 1024 * 1024L);
		public static final Pair<String, Short> REPLICATION_FACTOR = new Pair<String, Short>("ReplicationFactor", (short) 3);
	}
}
