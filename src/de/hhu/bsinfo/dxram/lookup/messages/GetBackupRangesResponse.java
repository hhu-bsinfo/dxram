
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a GetBackupRangesRequest
 * @author Kevin Beineke
 *         08.10.2015
 */
public class GetBackupRangesResponse extends AbstractResponse {

	// Attributes
	private BackupRange[] m_backupRanges;

	// Constructors
	/**
	 * Creates an instance of GetBackupRangesResponse
	 */
	public GetBackupRangesResponse() {
		super();

		m_backupRanges = null;
	}

	/**
	 * Creates an instance of GetBackupRangesResponse
	 * @param p_request
	 *            the corresponding GetBackupRangesRequest
	 * @param p_backupRanges
	 *            all backup ranges for requested NodeID
	 */
	public GetBackupRangesResponse(final GetBackupRangesRequest p_request, final BackupRange[] p_backupRanges) {
		super(p_request, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_RESPONSE);

		m_backupRanges = p_backupRanges;
	}

	// Getters
	/**
	 * Get all backup ranges
	 * @return all backup ranges
	 */
	public final BackupRange[] getBackupRanges() {
		return m_backupRanges;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		final MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

		p_buffer.putInt(m_backupRanges.length);
		for (BackupRange backupRange : m_backupRanges) {
			exporter.setPayloadSize(backupRange.sizeofObject());
			exporter.exportObject(backupRange);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		final MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

		m_backupRanges = new BackupRange[p_buffer.getInt()];
		for (int i = 0; i < m_backupRanges.length; i++) {
			m_backupRanges[i] = new BackupRange();
			importer.importObject(m_backupRanges[i]);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + BackupRange.sizeofObjectStatic() * m_backupRanges.length;
	}

}
