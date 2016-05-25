
package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import de.hhu.bsinfo.utils.Pair;

/**
 * Implementation of a writer to write vertex data to a text file.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
class FileWriterTextThread extends AbstractFileWriterThread {

	/**
	 * Constructor
	 *
	 * @param p_outputPath       Output file to write to.
	 * @param p_id               Id of the writer (0 based index).
	 * @param p_idRangeStartIncl Range of vertex ids to write to the file, start.
	 * @param p_idRangeEndExcl   Range of the vertex ids to write the file, end.
	 * @param p_storage          Storage to access for vertex data to write to the file.
	 */
	FileWriterTextThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		super(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}

	@Override
	public void run() {
		try {
			File file = new File(m_outputPath + "out.oel." + m_id);
			if (file.exists()) {
				if (!file.delete()) {
					System.out.println("Deleting file " + file + " failed.");
					m_errorCode = -1;
				}
			}

			File fileInfo = new File(m_outputPath + "out.ioel." + m_id);
			if (fileInfo.exists()) {
				if (!fileInfo.delete()) {
					System.out.println("Deleting file " + file + " failed.");
					m_errorCode = -2;
				}
			}

			BufferedWriter raf = new BufferedWriter(new FileWriter(file));
			BufferedWriter raf2 = new BufferedWriter(new FileWriter(fileInfo));
			if (!dumpOrdered(raf, raf2, m_idRangeStartIncl, m_idRangeEndExcl)) {
				System.out.println(
						"Dumping from vertex storage [" + m_idRangeStartIncl + ", " + m_idRangeEndExcl + "] failed.");
				raf.close();
				raf2.close();
			}

			raf.close();
			raf2.close();
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
	 *
	 * @param p_file           File to write the vertex data to.
	 * @param p_infoFile       Info file with metadata.
	 * @param p_rangeStartIncl Vertex id range start to write.
	 * @param p_rangeEndExcl   Vertex id range end to write.
	 * @return True if successful, false on error.
	 */
	private boolean dumpOrdered(final BufferedWriter p_file, final BufferedWriter p_infoFile,
			final long p_rangeStartIncl, final long p_rangeEndExcl) {
		long edgeCount = 0;
		long vertexCount = 0;
		for (long i = p_rangeStartIncl; i < p_rangeEndExcl; i++) {
			Pair<Long, String> vertexNeighbourList = ((VertexStorageText) m_storage).getVertexNeighbourList(i);

			try {
				// p_file.write(Long.toString(i) + ": ");
				p_file.write(vertexNeighbourList.m_second);
				p_file.write("\n");
			} catch (final IOException e) {
				return false;
			}

			edgeCount += vertexNeighbourList.m_first;
			vertexCount++;
			updateProgress("TotalVerticesToFiles " + m_id, vertexCount, p_rangeEndExcl - p_rangeStartIncl);
		}

		try {
			p_infoFile.write("," + Long.toString(edgeCount));
			p_file.flush();
			p_infoFile.flush();
		} catch (final IOException ignored) {
		}

		return true;
	}

}
