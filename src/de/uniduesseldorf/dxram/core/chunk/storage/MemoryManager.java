
package de.uniduesseldorf.dxram.core.chunk.storage;

import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.LIDElement;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.StatisticsManager;
import de.uniduesseldorf.dxram.utils.locks.JNIReadWriteSpinLock;

/**
 * Controls the access to the RawMemory and the CIDTable
 * @author Florian Klein
 *         13.02.2014
 */
// TODO better doc
// TODO get rid of exceptions? -> have proper return values like -1
// to indicate something failed?
// TODO get rid of references into core
public final class MemoryManager {

	private long m_nodeID;
	private SmallObjectHeap m_rawMemory;
	private CIDTable m_cidTable;
	private JNIReadWriteSpinLock m_lock;

	// Constructors
	/**
	 * Creates an instance of MemoryManager
	 */
	public MemoryManager(final long p_nodeID) 
	{
		m_nodeID = p_nodeID;
	}

	// Methods
	/**
	 * Initializes the RawMemory and the CIDTable
	 * @param p_size
	 *            the size of the memory
	 * @param p_segmentSize 
	 * 			  size of a single segment
	 * @param p_registerStatistics
	 * 			  True to register memory statistics, false otherwise.
	 * @return the actual size of the memory
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be initialized
	 */
	public long initialize(final long p_size, final long p_segmentSize, final boolean p_registerStatistics) throws MemoryException {
		long ret;

		if (p_registerStatistics) {
			StatisticsManager.registerStatistic("Memory", MemoryStatistic.getInstance());
		}

		m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory());
		ret = m_rawMemory.initialize(p_size, p_segmentSize);
		m_cidTable = new CIDTable(m_nodeID);
		m_cidTable.initialize(m_rawMemory);
		
		m_lock = new JNIReadWriteSpinLock();

		return ret;
	}

	/**
	 * Disengages the RawMemory and the CIDTable
	 * @throws MemoryException
	 *             if the RawMemory or the CIDTable could not be disengaged
	 */
	public void disengage() throws MemoryException {
		m_cidTable.disengage();
		m_rawMemory.disengage();

		m_cidTable = null;
		m_rawMemory = null;
		m_lock = null;
	}
	
	// TODO doc and also document which
	// functions are management calls
	// and which are access calls
	public void lockManage()
	{
		m_lock.writeLock().lock();
	}
	
	public void lockAccess()
	{
		m_lock.readLock().lock();
	}
	
	public void unlockManage()
	{
		m_lock.writeLock().unlock();
	}
	
	public void unlockAccess()
	{
		m_lock.readLock().unlock();
	}
	
	private int getSizeVersion(final int p_version)
	{
		assert p_version >= 0;
	
		int versionSize = -1;
		
		// max supported 2^28
		// versions 0 and 1 are stored using custom
		// state
		if (p_version <= 1) {
			versionSize = 0;
		// 2^7
		} else if (p_version <= 0x7F) {
			versionSize = 1;
		// 2^14
		} else if (p_version <= 0x4000) {
			versionSize = 2;
		// 2^21
		} else if (p_version <= 0x200000) {
			versionSize = 3;
		// 2^28
		} else if (p_version <= 10000000) {
			versionSize = 4;
		}
		
		return versionSize;
	}
	
	// make sure the block at address is big enough to also fit the address
	// at the beginning of the payload!
	private void writeVersion(final long p_address, final int p_version) throws MemoryException
	{
		int versionSize = -1;
		int versionToWrite = -1;
		
		versionToWrite = p_version;
		versionSize = getSizeVersion(p_version);
		// overflow, reset version
		if (versionSize == -1)
		{
			versionToWrite = 0;
			versionSize = 0;
		}
		
		switch (versionSize)
		{
			case 0:
				m_rawMemory.setCustomState(p_address, versionToWrite);
				break;
			case 1:
				byte b0;
				
				b0 = (byte) (versionToWrite & 0x7F);
				
				m_rawMemory.setCustomState(p_address, 2);
				m_rawMemory.writeByte(p_address, 0, b0);
				break;
			case 2:
				byte b1;
				
				b0 = (byte) ((versionToWrite & 0x7F) | 0x80);
				b1 = (byte) ((versionToWrite >> 7) & 0x7F);
 				
				m_rawMemory.setCustomState(p_address, 2);
				
				m_rawMemory.writeByte(p_address, 0, b0);
				m_rawMemory.writeByte(p_address, 1, b1);
				break;
			case 3:
				byte b2;
				
				b0 = (byte) ((versionToWrite & 0x7F) | 0x80);
				b1 = (byte) (((versionToWrite >> 7) & 0x7F) | 0x80);
				b2 = (byte) (((versionToWrite >> 14) & 0x7F));
 				
				m_rawMemory.setCustomState(p_address, 2);
				
				m_rawMemory.writeByte(p_address, 0, b0);
				m_rawMemory.writeByte(p_address, 1, b1);
				m_rawMemory.writeByte(p_address, 2, b2);
				break;
			case 4:
				byte b3;
				
				b0 = (byte) ((versionToWrite & 0x7F) | 0x80);
				b1 = (byte) (((versionToWrite >> 7) & 0x7F) | 0x80);
				b2 = (byte) (((versionToWrite >> 14) & 0x7F) | 0x80);
				b3 = (byte) (((versionToWrite >> 21) & 0x7F));
 				
				m_rawMemory.setCustomState(p_address, 2);
				
				m_rawMemory.writeByte(p_address, 0, b0);
				m_rawMemory.writeByte(p_address, 1, b1);
				m_rawMemory.writeByte(p_address, 2, b2);
				m_rawMemory.writeByte(p_address, 3, b3);
				break;
			default:
				assert 1 == 2;
				break;
		}
	}
	
	private int readVersion(final long p_address) throws MemoryException
	{
		int version = -1;
		int customState = -1;
		
		customState = m_rawMemory.getCustomState(p_address);
		
		switch (customState)
		{
		case 0:
			version = 0;
			break;
		case 1:
			version = 1;
			break;
		case 2:
			byte b0 = 0; 
			byte b1 = 0;
			byte b2 = 0;
			byte b3 = 0;
			
			b0 = m_rawMemory.readByte(p_address, 0);
			if ((b0 & 0x80) == 1) 
			{
				b1 = m_rawMemory.readByte(p_address, 1);
				if ((b1 & 0x80) == 1)
				{
					b2 = m_rawMemory.readByte(p_address, 2);
					if ((b2 & 0x80) == 1)
					{
						b3 = m_rawMemory.readByte(p_address, 3);
					}
				}
			}
			
			version = 	((b3 & 0x7F) << 21) | 
						((b2 & 0x7F) << 14) | 
						((b1 & 0x7F) << 7) | 
						(b0 & 0x7F);
			break;
		default:
			assert 1 == 2;
			break;
		}
		
		return version;
	}

	// write lock
	public long create(final int p_size) throws MemoryException
	{
		assert p_size > 0;
		
		long address = -1;
		long chunkID = -1;
		int chunkSize = -1;
		LIDElement lid = null;
		
		// get new LID from CIDTable
		lid = m_cidTable.getFreeLID();
		
		assert lid.getVersion() >= 0;
		
		chunkSize = p_size + getSizeVersion(lid.getVersion());		
			
		// first, try to allocate. maybe early return
		address = m_rawMemory.malloc(chunkSize);
		if (address >= 0) {
			// register new chunk
			chunkID = ((long) m_nodeID << 48) + lid.getLocalID();
			m_cidTable.set(chunkID, address);
			
			writeVersion(address, lid.getVersion());
		}
		else
		{
			// put lid back
			// TODO is that ok? that's a protected call...
			// check CID table impl
			m_cidTable.putChunkIDForReuse(lid.getLocalID(), lid.getVersion());
		}

		return chunkID;
	}
	
	// gets the size of the block (payload only, i.e. minus size for version
	// read lock
	public int getSize(final long p_chunkID) throws MemoryException
	{
		long address = -1;
		int size = -1;
		
		address = m_cidTable.get(p_chunkID);
		if (address > 0)
		{
			size = m_rawMemory.getSizeBlock(address) - getSizeVersion(readVersion(address));
		}
		
		return size;
	}
	
	// read lock
	public int get(final long p_chunkID, byte[] p_buffer, int p_offset, int p_length) throws MemoryException
	{
		long address;
		int bytesRead = -1;
		
		address = m_cidTable.get(p_chunkID);
		if (address > 0)
		{
			int sizeVersion;
			
			sizeVersion = getSizeVersion(readVersion(address));
			bytesRead = m_rawMemory.readBytes(address, sizeVersion, p_buffer, p_offset, p_length);
		}
		
		return bytesRead;
	}
	
	// read lock
	public int getVersion(final long p_chunkID) throws MemoryException
	{
		long address = 0;
		int version = -1;
		
		address = m_cidTable.get(p_chunkID);
		if (address > 0)
		{
			version = readVersion(address);
		}
		
		return version;
	}
	
	// FIXME: put needs to be write locked, because it
	// can reallocate memory due to payload size increase (version increment)
	// and incrementing the version
	public int put(final long p_chunkID, byte[] p_buffer, int p_offset, int p_length) throws MemoryException
	{		
		long address;
		int bytesWritten = -1;
		
		address = m_cidTable.get(p_chunkID);
		if (address > 0)
		{
			int curVersion = -1;
			int sizeVersion = -1;
			int sizeVersionNext = -1;
			
			curVersion = readVersion(address);
			sizeVersion = getSizeVersion(curVersion);
			sizeVersionNext = getSizeVersion(curVersion + 1);
			
			// check if we have to reallocate due to version growth
			if (sizeVersion != sizeVersionNext)
			{
				int newTotalSizePayload;
				long newAddress = -1;
				
				// skip version and read payload
				byte[] payload = m_rawMemory.readBytes(address, sizeVersion);
				newTotalSizePayload = payload.length + sizeVersionNext;
				
				// try to allocate first
				newAddress = m_rawMemory.malloc(newTotalSizePayload);
				if (newAddress != -1)
				{
					if (m_rawMemory.writeBytes(address, sizeVersionNext, payload) != payload.length)
					{
						// revert
						m_rawMemory.free(newAddress);
					}
					else
					{
						m_rawMemory.free(address);
						m_cidTable.set(p_chunkID, newAddress);
						address = newAddress;
					}
				}
			}
			
			writeVersion(address, curVersion + 1);
			bytesWritten = m_rawMemory.writeBytes(address, sizeVersion, p_buffer, p_offset, p_length);
		}
		
		return bytesWritten;
	}

	// make sure to write lock this call
	public boolean delete(final long p_chunkID) throws MemoryException
	{
		long address;
		boolean deleted = false;
		
		address = m_cidTable.get(p_chunkID);
		if (address > 0)
		{
			m_rawMemory.free(address);
			deleted = true;
		}
		
		return deleted;
	}
	
	// TODO
//	public int resize(final long p_chunkID, final int p_newSize)
//	{
//		
//	}
	
	// TODO remove
//	/**
//	 * Gets the next free local ID
//	 * @return the next free local ID
//	 */
//	public CIDElement getNextLocalID() {
//
//
//		return ret;
//	}
//
//	/**
//	 * Gets the next free local IDs
//	 * @param p_count
//	 *            the number of free local IDs
//	 * @return the next free local IDs
//	 */
//	public CIDElement[] getNextLocalIDs(final int p_count) {
//		CIDElement[] ret = null;
//		int count = 0;
//		long localID;
//
//		ret = new CIDElement[p_count];
//		for (int i = 0; i < p_count; i++) {
//			try {
//				ret[i] = m_cidTable.getFreeLID();
//			} catch (final DXRAMException e) {}
//			if (ret[i] == null) {
//				count++;
//			}
//		}
//
//		localID = m_nextLocalID.getAndAdd(count);
//		count = 0;
//		for (int i = 0; i < p_count; i++) {
//			if (ret[i] == null) {
//				ret[i] = new CIDElement(localID + count++, 0);
//			}
//		}
//
//		return ret;
//	}
//
//	/**
//	 * Gets the current local ID
//	 * @return the current local ID
//	 */
//	public long getCurrentLocalID() {
//		long ret;
//
//		ret = m_nextLocalID.get() - 1;
//
//		return ret;
//	}



//	public Chunk resize(final long p_chunkID, final long p_newSize) throws MemoryException {
//		Chunk ret = null;
//		long address = -1;
//
//		// Get the address from the CIDTable
//		address = m_cidTable.get(p_chunkID);
//
//		// If address <= 0, the Chunk does not exists in the memory
//		if (address > 0) {
//			long newAddress = -1;
//
//			// try reallocating the block
//			newAddress = m_rawMemory.realloc(address, p_newSize);
//			if (newAddress != address)
//			{
//				int customState = -1;
//				byte[] data = null;
//
//				// copy data and free old block
//				customState = m_rawMemory.getCustomState(address);
//				data = m_rawMemory.readBytes(address);
//
//				m_rawMemory.setCustomState(newAddress, customState);
//
//				// if the new block is smaller, trunc data
//				if (p_newSize < data.length)
//					m_rawMemory.writeBytes(newAddress, data, p_);
//				else
//					m_rawMemory.writeBytes(newAddress, data);
//			}
//
//
//			int version;
//			int sizeVersion;
//			byte[] data;
//
//			sizeVersion = m_rawMemory.getCustomState(address) + 1;
//			version = readVersion(address, sizeVersion);
//
//			// make sure to skip version data
//			data = m_rawMemory.readBytes(address, sizeVersion);
//
//			ret = new Chunk(p_chunkID, data, version);
//		}
//
//		return ret;
//	}

	// TODO doc: this function needs to be read locked at least
	/**
	 * Returns whether this Chunk is stored locally or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk is stored locally or not
	 * @throws MemoryException
	 *             if the Chunk could not be checked
	 */
	public boolean isResponsible(final long p_chunkID) throws MemoryException {
		long address;

		// Get the address from the CIDTable
		address = m_cidTable.get(p_chunkID);

		// If address <= 0, the Chunk does not exists in the memory
		return address > 0;
	}

	/**
	 * Returns whether this Chunk was migrated here or not
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this Chunk was migrated here or not
	 */
	public boolean wasMigrated(final long p_chunkID) {
		return ChunkID.getCreatorID(p_chunkID) != NodeID.getLocalNodeID();
	}

	/**
	 * Removes a Chunk from the memory
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public void remove(final long p_chunkID) throws MemoryException {
		int version;
		int sizeVersion;
		long addressDeletedChunk;

		// Get and delete the address from the CIDTable, mark as zombie first
		addressDeletedChunk = m_cidTable.delete(p_chunkID, true);

		// read version
		sizeVersion = m_rawMemory.getCustomState(addressDeletedChunk) + 1;
		version = readVersion(addressDeletedChunk, sizeVersion);

		// more space for another zombie for reuse in LID store?
		if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID), version)) {
			// detach reference to zombie and free memory
			m_cidTable.delete(p_chunkID, false);
			m_rawMemory.free(addressDeletedChunk);
		} else {
			// no space for zombie in LID store, keep him "alive" in memory
		}
	}

	/**
	 * Removes the ChunkID of a deleted Chunk that was migrated
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_version
	 *            the version
	 * @throws MemoryException
	 *             if the Chunk could not be get
	 */
	public void prepareChunkIDForReuse(final long p_chunkID, final int p_version) throws MemoryException {
		m_cidTable.putChunkIDForReuse(p_chunkID, p_version);
	}

	// FIXME take back in
//	/**
//	 * Returns the ChunkIDs of all migrated Chunks
//	 * @return the ChunkIDs of all migrated Chunks
//	 * @throws MemoryException
//	 *             if the CIDTable could not be completely accessed
//	 */
//	public ArrayList<Long> getCIDOfAllMigratedChunks() throws MemoryException {
//		return m_cidTable.getCIDOfAllMigratedChunks();
//	}
//
//	/**
//	 * Returns the ChunkID ranges of all locally stored Chunks
//	 * @return the ChunkID ranges in an ArrayList
//	 * @throws MemoryException
//	 *             if the CIDTable could not be completely accessed
//	 */
//	public ArrayList<Long> getCIDrangesOfAllLocalChunks() throws MemoryException {
//		return m_cidTable.getCIDrangesOfAllLocalChunks();
//	}

	/**
	 * Write the version number to the specified position.
	 * @param p_address
	 *            Address to write version number to.
	 * @param p_version
	 *            Version number/value.
	 * @param p_size
	 *            Storage size this number needs.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	protected void writeVersion(final long p_address, final int p_version, final int p_size)
			throws MemoryException {
		switch (p_size) {
		case 1:
			m_rawMemory.writeByte(p_address, (byte) (p_version & 0xFF));
			break;
		case 2:
			m_rawMemory.writeShort(p_address, (short) (p_version & 0xFFFF));
			break;
		case 3:
			// store as big endian
			m_rawMemory.writeByte(p_address, (byte) ((p_version >> 16) & 0xFF));
			m_rawMemory.writeShort(p_address, 2, (short) (p_version & 0xFFFF));
			break;
		default:
			assert 1 == 2;
			break;
		}
	}

	/**
	 * Read the version number from the specified location.
	 * @param p_address
	 *            Address to read the version number from.
	 * @param p_size
	 *            Storage size of the version number to read.
	 * @return Version number read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	protected int readVersion(final long p_address, final int p_size) throws MemoryException {
		int ret;

		switch (p_size) {
		case 1:
			ret = (int) m_rawMemory.readByte(p_address) & 0xFF;
			break;
		case 2:
			ret = (int) m_rawMemory.readShort(p_address) & 0xFF;
			break;
		case 3:
			int tmp;

			tmp = 0;
			tmp |= (m_rawMemory.readByte(p_address) << 16) & 0xFF;
			tmp |= m_rawMemory.readShort(p_address, 2) & 0xFF;
			ret = tmp;
			break;
		default:
			assert 1 == 2;
			ret = -1;
			break;
		}

		return ret;
	}
}
