
package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import de.hhu.bsinfo.utils.Pair;

public class FileWriterTextThread extends FileWriterThread {
	public FileWriterTextThread(final String p_outputPath, final int p_id, final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		super(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}

	@Override
	public void run() {
		try {
			File file = new File(m_outputPath + "out.oel." + m_id);
			if (file.exists()) {
				file.delete();
			}

			File fileInfo = new File(m_outputPath + "out.ioel." + m_id);
			if (fileInfo.exists()) {
				fileInfo.delete();
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

	private boolean dumpOrdered(final BufferedWriter p_file, final BufferedWriter p_infoFile,
			final long p_rangeStartIncl, final long p_rangeEndExcl) {
		// write header (count)
		try {
			p_file.write(Long.toString(p_rangeEndExcl - p_rangeStartIncl) + "\n");
			p_infoFile.write(Integer.toString(m_id));
			p_infoFile.write("," + Long.toString(p_rangeEndExcl - p_rangeStartIncl));
		} catch (final IOException e) {
			return false;
		}

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
		} catch (final IOException e) {}

		return true;
	}

}
