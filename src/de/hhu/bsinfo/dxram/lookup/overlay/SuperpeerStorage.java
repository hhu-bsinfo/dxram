
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * An object/chunk storage on the superpeer for temporary data which is not wanted in the normal
 * chunk storage. This allows us to map any ids to the objects to store. But the max number of items
 * and the total size of the storage is limited to avoid abusing this as a primary storage for data.
 * Also, the chunks stored here are NOT covered by the backup/recovery but are replicated to other superpeers
 * to cover superpeer failure (though a full system failure will lose all stored data)
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class SuperpeerStorage {

	private int m_maxNumEntries;
	private int m_maxSizeBytes;

	private HashMap<Integer, byte[]> m_storage = new HashMap<>();
	private int m_allocatedSizeBytes;
	private int m_entryCount;

	/**
	 * Constructor
	 * @param p_maxNumEntries
	 *            Max num of entries allowed in the storage.
	 * @param p_maxSizeBytes
	 *            Max size of bytes the objects are allowed to consume (all together)
	 */
	SuperpeerStorage(final int p_maxNumEntries, final int p_maxSizeBytes) {
		m_maxNumEntries = p_maxNumEntries;
		m_maxSizeBytes = p_maxSizeBytes;
		m_entryCount = 0;
	}

	/**
	 * Create a new block to store a chunk.
	 * @param p_id
	 *            Id to assign to the block
	 * @param p_size
	 *            Size of the block
	 * @return 0 on success, -1 if quota reached, -2 if max num entries reached, -3 if id already in use.
	 */
	int create(final int p_id, final int p_size) {
		if (m_allocatedSizeBytes + p_size > m_maxSizeBytes) {
			return -1;
		}

		if (m_entryCount + 1 >= m_maxNumEntries) {
			return -2;
		}

		if (m_storage.containsKey(p_id)) {
			return -3;
		}

		m_storage.put(p_id, new byte[p_size]);
		m_allocatedSizeBytes += p_size;
		m_entryCount++;

		return 0;
	}

	/**
	 * Put data into an allocated block.
	 * @param p_id
	 *            Id of the block.
	 * @param p_data
	 *            Data to put.
	 * @return Number of bytes written to the block or -1 if the block does not exist.
	 */
	int put(final int p_id, final byte[] p_data) {
		byte[] data = m_storage.get(p_id);
		if (data == null) {
			return -1;
		}

		int written;
		if (p_data.length > data.length) {
			written = data.length;
		} else {
			written = p_data.length;
		}

		System.arraycopy(p_data, 0, data, 0, written);

		return written;
	}

	/**
	 * Get data from an allocated block
	 * @param p_id
	 *            Id of the block
	 * @return Data read from the memory block or null if id does not point to an allocated block.
	 */
	byte[] get(final int p_id) {
		byte[] data = m_storage.get(p_id);
		if (data == null) {
			return null;
		}

		return data;
	}

	/**
	 * Remove a block from the storage (i.e. delete it).
	 * @param p_id
	 *            Id of the block to delete.
	 * @return False if the block does not exist, true on success.
	 */
	boolean remove(final int p_id) {
		byte[] data = m_storage.remove(p_id);
		if (data != null) {
			m_entryCount--;
			m_allocatedSizeBytes -= data.length;
			return true;
		}

		return false;
	}

	/**
	 * Get the current status of the storage (i.e. allocations).
	 * @return Current status of the storage.
	 */
	Status getStatus() {
		ArrayList<Long> statusArray = new ArrayList<>(m_storage.size());
		for (Map.Entry<Integer, byte[]> entry : m_storage.entrySet()) {
			long val = (((long) entry.getKey()) << 32L) | entry.getValue().length;
			statusArray.add(val);
		}

		return new Status(m_maxNumEntries, m_maxSizeBytes, statusArray);
	}

	/**
	 * Status of the superpeer storage (allocations)
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
	 */
	public static class Status implements Exportable, Importable {
		private int m_maxNumItems;
		private int m_maxStorageSizeBytes;
		private ArrayList<Long> m_storageStatus;

		public Status() {
			m_storageStatus = new ArrayList<Long>();
		}

		/**
		 * Constructor
		 * @param p_maxNumItems
		 *            Max number of items allowed in the storage.
		 * @param p_maxStorageSizeBytes
		 *            Max storage size in bytes.
		 * @param p_storageStatus
		 *            Storage status i.e. List of Ids + size of the blocks allocated.
		 */
		public Status(final int p_maxNumItems, final int p_maxStorageSizeBytes, final ArrayList<Long> p_storageStatus) {
			m_maxNumItems = p_maxNumItems;
			m_maxStorageSizeBytes = p_maxStorageSizeBytes;
			m_storageStatus = p_storageStatus;
		}

		/**
		 * Get the item limit for the storage.
		 * @return Item limit of the storage.
		 */
		public int getMaxNumItems() {
			return m_maxNumItems;
		}

		/**
		 * Get the max size of the storage in bytes.
		 * @return Max size on bytes.
		 */
		public int getMaxStorageSizeBytes() {
			return m_maxStorageSizeBytes;
		}

		/**
		 * Get the storage status. One long value consists of two ints.
		 * The higher int contains the id of an allocated chunk, the lower int
		 * the size of the chunk allocated.
		 * @return List of allocated "chunk states".
		 */
		public ArrayList<Long> getStatusArray() {
			return m_storageStatus;
		}

		/**
		 * Calculate the total amount of bytes used for storing data based on the allocation status.
		 * @return Total amount of space occupied by actual data.
		 */
		public int calculateTotalDataUsageBytes() {
			int size = 0;
			for (long val : m_storageStatus) {
				size += (int) val;
			}

			return size;
		}

		@Override
		public String toString() {
			String str = "Id: Size in bytes";

			for (long val : m_storageStatus) {
				str += "\n" + ChunkID.toHexString((int) (val >> 32)) + ": " + ((int) val);
			}

			return str;
		}

		@Override
		public int exportObject(final Exporter p_exporter, final int p_size) {
			p_exporter.writeInt(m_maxNumItems);
			p_exporter.writeInt(m_maxStorageSizeBytes);
			p_exporter.writeInt(m_storageStatus.size());
			for (Long status : m_storageStatus) {
				long val = status;
				p_exporter.writeInt((int) (val >> 32L));
				p_exporter.writeInt((int) val);
			}

			return sizeofObject();
		}

		@Override
		public int importObject(final Importer p_importer, final int p_size) {
			m_maxNumItems = p_importer.readInt();
			m_maxStorageSizeBytes = p_importer.readInt();
			int size = p_importer.readInt();
			m_storageStatus = new ArrayList<>(size);
			for (int i = 0; i < size; i++) {
				m_storageStatus.add((((long) p_importer.readInt()) << 32L) | p_importer.readInt());
			}

			return sizeofObject();
		}

		@Override
		public int sizeofObject() {
			return Integer.BYTES * 3 + m_storageStatus.size() * Long.BYTES;
		}

		@Override
		public boolean hasDynamicObjectSize() {
			return true;
		}
	}
}
