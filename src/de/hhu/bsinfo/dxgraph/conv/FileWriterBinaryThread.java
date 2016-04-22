
package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of a writer to write vertex data to a binary file.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public class FileWriterBinaryThread extends AbstractFileWriterThread {
	/**
	 * Constructor
	 * @param p_outputPath
	 *            Output file to write to.
	 * @param p_id
	 *            Id of the writer (0 based index).
	 * @param p_idRangeStartIncl
	 *            Range of vertex ids to write to the file, start.
	 * @param p_idRangeEndExcl
	 *            Range of the vertex ids to write the file, end.
	 * @param p_storage
	 *            Storage to access for vertex data to write to the file.
	 */
	public FileWriterBinaryThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		super(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}

	@Override
	public void run() {
		try {
			File file = new File(m_outputPath + "out.boel." + m_id);
			if (file.exists()) {
				file.delete();
			}

			File fileInfo = new File(m_outputPath + "out.ioel." + m_id);
			if (fileInfo.exists()) {
				fileInfo.delete();
			}

			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			BufferedWriter out2 = new BufferedWriter(new FileWriter(fileInfo));
			if (!dumpOrdered(out, out2, m_idRangeStartIncl, m_idRangeEndExcl)) {
				System.out.println(
						"Dumping from vertex storage [" + m_idRangeStartIncl + ", " + m_idRangeEndExcl + "] failed.");
				out.close();
				out2.close();
			}

			out.close();
			out2.close();
		} catch (final IOException e) {
			System.out.println("Dumping to out file failed: " + e.getMessage());
			m_errorCode = -2;
			return;
		}

		System.out.println("Dumping [" + m_idRangeStartIncl + ", " + m_idRangeEndExcl + "] to file done");
		m_errorCode = 0;
	}

	/**
	 * Write the vertex data to the file in ascending vertex id order. Also creates info file with metadata.
	 * @param p_file
	 *            File to write the vertex data to.
	 * @param p_infoFile
	 *            Info file with metadata.
	 * @param p_rangeStartIncl
	 *            Vertex id range start to write.
	 * @param p_rangeEndExcl
	 *            Vertex id range end to write.
	 * @return True if successful, false on error.
	 */
	private boolean dumpOrdered(final DataOutputStream p_file, final BufferedWriter p_infoFile,
			final long p_rangeStartIncl, final long p_rangeEndExcl) {
		long edgeCount = 0;
		long vertexCount = 0;
		for (long i = p_rangeStartIncl; i < p_rangeEndExcl; i++) {
			ConcurrentLinkedQueue<Long> vertexNeighbourList =
					((VertexStorageBinarySimple) m_storage).getVertexNeighbourList(i);
			int neighbourCount = vertexNeighbourList.size();

			try {
				p_file.writeInt(neighbourCount);
				for (long id : vertexNeighbourList) {
					p_file.writeLong(id);
				}
			} catch (final IOException e) {
				return false;
			}

			edgeCount += neighbourCount;
			vertexCount++;
			updateProgress("TotalVerticesToFiles " + m_id, vertexCount, p_rangeEndExcl - p_rangeStartIncl);
		}

		try {
			p_infoFile.write("," + Long.toString(edgeCount));
			p_file.flush();
			p_infoFile.flush();
		} catch (final IOException e) {}

		return true;
	}

}
