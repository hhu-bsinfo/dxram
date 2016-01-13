package de.hhu.bsinfo.dxram.engine;

import de.hhu.bsinfo.utils.Pair;

public class DXRAMEngineConfigurationValues {
	
	// DXRAM role (Superpeer, Peer or Monitor)
	public static final Pair<String, String> IP = new Pair<String, String>("IP", "127.0.0.1");
	public static final Pair<String, Integer> PORT = new Pair<String, Integer>("Port", 22221);
	public static final Pair<String, String> ROLE = new Pair<String, String>("Role", "Peer");
	public static final Pair<String, String> LOGGER = new Pair<String, String>("Logger", "de.hhu.bsinfo.dxram.util.logger.LoggerLog4j");
	public static final Pair<String, String> LOG_LEVEL = new Pair<String, String>("LogLevel", "trace");
	public static final Pair<String, String> JNI_LOCK_PATH = new Pair<String, String>("JNI/JNILock", "/opt/dxram/lib/libJNILock.so");
}
