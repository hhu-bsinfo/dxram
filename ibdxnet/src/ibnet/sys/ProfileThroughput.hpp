/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf,
 * Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

#ifndef IBNET_SYS_PROFILETHROUGHPUT_HPP
#define IBNET_SYS_PROFILETHROUGHPUT_HPP

#include "ProfileTimer.hpp"

namespace ibnet {
namespace sys {

/**
 * Measure the throughput of data processing, e.g. incoming/outgoing
 * network packages.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class ProfileThroughput
{
public:
    /**
     * Constructor
     *
     * @param resSec Resolution in seconds for throughput
     * @param name Name to identify this meter (for debug output)
     */
    ProfileThroughput(uint32_t resSec = 1, const std::string& name = "") :
        m_resSec(resSec),
        m_name(name),
        m_totalTimer(),
        m_sliceTimer(),
        m_totalData(0),
        m_sliceData(0),
        m_updateCounter(0),
        m_lastThroughput(0),
        m_peakThroughput(0),
        m_avgCounter(0),
        m_avgThroughputSum(0)
    {

    }

    /**
     * Destructor
     */
    ~ProfileThroughput(void) {};

    /**
     * Start measuring throughput. Call this once before you want to start
     * updating this meter.
     */
    void Start(void)
    {
        m_totalTimer.Enter();
        m_sliceTimer.Enter();
    }

    /**
     * Check if this meter is already started
     */
    bool IsStarted(void) const {
        return m_totalTimer.GetCounter() > 0;
    }

    /**
     * Update the meter with data
     *
     * @param bytes Number of bytes moved, copied, processed etc
     */
    void Update(uint32_t bytes)
    {
        m_totalData += bytes;
        m_sliceData += bytes;
        m_updateCounter++;

        m_sliceTimer.Exit();
        if (m_sliceTimer.GetTotalTime() > m_resSec) {
            m_lastThroughput = m_sliceData / m_sliceTimer.GetTotalTime();

            if (m_peakThroughput < m_lastThroughput) {
                m_peakThroughput = m_lastThroughput;
            }

            m_avgCounter++;
            m_avgThroughputSum += m_lastThroughput;
        }
        m_sliceTimer.Enter();
    }

    /**
     * Get the throughput of the last slice based on the values provided
     * by calling udpate()
     *
     * @return Throughput of bytes of last slice
     */
    uint64_t GetLastThroughput(void) const {
        return m_lastThroughput;
    }

    /**
     * Get the current peak throughput measured so far (on a time slice basis)
     *
     * @return Peak throughput in bytes
     */
    uint64_t GetCurrentPeak(void) const {
        return m_peakThroughput;
    }

    /**
     * Get the current average throughput. Based on the total number of bytes
     * updated so far.
     *
     * @return Avarage throughput in bytes
     */
    uint64_t GetCurrentAvg(void) const {
        if (m_avgCounter == 0) {
            return 0;
        }

        return m_avgThroughputSum / m_avgCounter;
    }

    /**
     * Get the number of times the update function was called so far
     */
    uint64_t GetUpdateCount(void) const {
        return m_updateCounter;
    }

    /**
     * Stop the meter. Call this when you stopped processing data to also stop
     * the total timer of the meter
     */
    void Stop(void)
    {
        m_sliceTimer.Exit();
        m_totalTimer.Exit();
    }

    /**
     * Enable output to an out stream
     */
    friend std::ostream &operator<<(std::ostream& os,
            const ProfileThroughput& o) {
        return os << o.m_name <<
            " Total data: " << __FormatData(o.m_totalData) <<
            ", Last throughput: " << __FormatData(o.GetLastThroughput()) <<
            "/sec, Peak throughput: " << __FormatData(o.GetCurrentPeak()) <<
            "/sec, Avg throughput: " << __FormatData(o.GetCurrentAvg()) <<
            "/sec";
    }

private:
    uint32_t m_resSec;
    const std::string m_name;

    ProfileTimer m_totalTimer;
    ProfileTimer m_sliceTimer;

    uint64_t m_totalData;
    uint64_t m_sliceData;
    uint64_t m_updateCounter;

    uint64_t m_lastThroughput;
    uint64_t m_peakThroughput;
    uint64_t m_avgCounter;
    uint64_t m_avgThroughputSum;

    static std::string __FormatData(uint64_t bytes)
    {
        if (bytes < 1024) {
            return std::to_string(bytes) + " b";
        } else if (bytes < 1024 * 1024) {
            return std::to_string(bytes / 1024.0) + " kb";
        } else if (bytes < 1024 * 1024 * 1024) {
            return std::to_string(bytes / 1024.0 / 1024.0) + " mb";
        } else {
            return std::to_string(bytes / 1024.0 / 1024.0 / 1024.0) + " gb";
        }
    }
};

}
}

#endif //IBNET_SYS_PROFILETHROUGHPUT_HPP
