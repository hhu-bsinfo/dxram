/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraph.conv;

/**
 * Base class for all threads doing conversion tasks.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.02.2016
 */
public class ConverterThread extends Thread {
    private long m_prevTime;
    protected int m_errorCode;

    /**
     * Constructor
     * @param p_name
     *            Thread name.
     */
    public ConverterThread(final String p_name) {
        super(p_name);
    }

    /**
     * Get the error code of the thread.
     * @return Error code.
     */
    public int getErrorCode() {
        return m_errorCode;
    }

    /**
     * Update and print (in intervals) the current progress of the conversion tasks.
     * @param p_msg
     *            Message to print for the progress.
     * @param p_curCount
     *            Current count of the task.
     * @param p_totalCount
     *            Total target count to reach of the task.
     */
    protected void updateProgress(final String p_msg, final long p_curCount, final long p_totalCount) {
        float curProgress = ((float) p_curCount) / p_totalCount;
        long curTime = System.currentTimeMillis();
        if (curTime - m_prevTime > 1000) {
            m_prevTime = curTime;
            if (curProgress > 1.0f) {
                curProgress = 1.0f;
            }

            System.out.println("Progress(" + p_msg + "): " + curProgress * 100 + "% (" + p_curCount + ")\r");
        }
    }
}
