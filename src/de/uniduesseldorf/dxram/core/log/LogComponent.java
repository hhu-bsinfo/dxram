package de.uniduesseldorf.dxram.core.log;

import de.uniduesseldorf.dxram.core.data.Chunk;
import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

import de.uniduesseldorf.utils.config.Configuration;

public class LogComponent extends DXRAMComponent
{
	public static final String COMPONENT_IDENTIFIER = "Log";
	
	public LogComponent(int p_priorityInit, int p_priorityShutdown) {
		super(COMPONENT_IDENTIFIER, p_priorityInit, p_priorityShutdown);

	}

	// ---------------------------------------------------------------------------------
	
	/**
	 * Initializes a new backup range
	 * @param p_firstChunkIDOrRangeID
	 *            the beginning of the range
	 * @param p_backupPeers
	 *            the backup peers
	 */
	public void initBackupRange(long p_firstChunkIDOrRangeID, short[] p_backupPeers) {
		// TODO
	}

	/**
	 * Recovers all Chunks of given backup range
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @return the recovered Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	public Chunk[] recoverBackupRange(short p_owner, long p_chunkID, byte p_rangeID) {
		// TODO
		return null;
	}

	/**
	 * Recovers all Chunks of given backup range
	 * @param p_fileName
	 *            the file name
	 * @param p_path
	 *            the path of the folder the file is in
	 * @return the recovered Chunks
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 */
	public Chunk[] recoverBackupRangeFromFile(String p_fileName, String p_path) {
		// TODO
		return null;
	}

	/**
	 * Prints the metadata of one node's log
	 * @param p_owner
	 *            the NodeID
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 * @note for testing only
	 */
	public void printBackupRange(short p_owner, long p_chunkID, byte p_rangeID) {
		// TODO
	}

	/**
	 * Returns the header size
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LocalID
	 * @param p_size
	 *            the size of the Chunk
	 * @return the header size
	 */
	public short getAproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
		// TODO 
		return 0;
	}

	// ---------------------------------------------------------------------------------

	@Override
	protected void registerConfigurationValuesComponent(Configuration p_configuration) {
		p_configuration.registerConfigurationEntries(LogConfigurationValues.CONFIGURATION_ENTRIES);
	}
	
	@Override
	protected boolean initComponent(Configuration p_configuration) {
		// TODO Auto-generated method stub
		return true;
	}
	
	@Override
	protected boolean shutdownComponent() {
		// TODO Auto-generated method stub
		return true;
	}
}
