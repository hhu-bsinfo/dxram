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

#ifndef IBNET_SYS_PROFILETIMER_HPP
#define IBNET_SYS_PROFILETIMER_HPP

#include <chrono>
#include <cstdint>
#include <iostream>

namespace ibnet {
namespace sys {

/**
 * Timer class to acurately measure time for profiling code sections
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class ProfileTimer
{
public:
    /**
     * Constructor
     *
     * @param name Name for this timer (to identify on debug output)
     */
    ProfileTimer(const std::string& name = "") :
        m_name(name),
        m_counter(0),
        m_total(std::chrono::nanoseconds(0)),
        m_worst(std::chrono::seconds(0)),
        // over 10 years is enough...
        m_best(std::chrono::hours(10000))
    {};

    /**
     * Destructor
     */
    ~ProfileTimer(void) {};

    /**
     * Reset the timer (set to 0)
     */
    void Reset(void)
    {
        m_counter = 0;
        m_total = std::chrono::nanoseconds(0);
        m_worst = std::chrono::seconds(0);
        // over 10 years is enough...
        m_best = std::chrono::hours(10000);
    }

    /**
     * Enter a section to be profiled/measured. Call this once, requires a call
     * to exit before calling again
     */
    void Enter(void)
    {
        m_counter++;
        m_enter = std::chrono::high_resolution_clock::now();
    }

    /**
     * Requires a call to enter first. Finish measuring the section to be
     * profiled
     */
    void Exit(void)
    {
        std::chrono::duration<uint64_t, std::nano> delta(std::chrono::high_resolution_clock::now() - m_enter);
        m_total += delta;

        if (delta < m_best) {
            m_best = delta;
        }

        if (delta > m_worst) {
            m_worst = delta;
        }
    }

    /**
     * Get number of times the seciton was profiled (enter was called)
     */
    uint64_t GetCounter(void) const {
        return m_counter;
    }

    /**
     * Get the total execution time of the section(s) enclosed by enter and
     * exit
     *
     * @return Total time in seconds
     */
    double GetTotalTime(void) const {
        return std::chrono::duration<double>(m_total).count();
    }

    /**
     * Get the total execution time of the section(s) enclosed by enter and
     * exit
     *
     * @tparam _Unit Time unit to return
     * @return Total time
     */
    template<typename _Unit>
    double GetTotalTime(void) const {
        return std::chrono::duration<double, _Unit>(m_total).count();
    }

    /**
     * Get the average execution time of the section(s) enclosed by enter and
     * exit
     *
     * @return Average execution time in seconds
     */
    double GetAvarageTime(void) const
    {
        if (m_counter == 0) {
            return 0;
        } else {
            return std::chrono::duration<double>(m_total).count() / m_counter;
        }
    }

    /**
     * Get the average execution time of the section(s) enclosed by enter and
     * exit
     *
     * @tparam _Unit Time unit to return
     * @return Total time
     */
    template<typename _Unit>
    double GetAvarageTime(void) const
    {
        if (m_counter == 0) {
            return 0;
        } else {
            return std::chrono::duration<double, _Unit>(m_total).count() / m_counter;
        }
    }

    /**
     * Get the best execution time of the section(s) enclosed by enter and
     * exit
     *
     * @return Best execution time in seconds
     */
    double GetBestTime(void) const {
        return std::chrono::duration<double>(m_best).count();
    }

    /**
     * Get the best execution time of the section(s) enclosed by enter and
     * exit
     *
     * @tparam _Unit Time unit to return
     * @return Best time
     */
    template<typename _Unit>
    double GetBestTime(void) const {
        return std::chrono::duration<double, _Unit>(m_best).count();
    }

    /**
     * Get the worst execution time of the section(s) enclosed by enter and
     * exit
     *
     * @return Worst execution time in seconds
     */
    double GetWorstTime(void) const {
        return std::chrono::duration<double>(m_worst).count();
    }

    /**
     * Get the worst execution time of the section(s) enclosed by enter and
     * exit
     *
     * @tparam _Unit Time unit to return
     * @return Worst time
     */
    template<typename _Unit>
    double GetWorstTime(void) const {
        return std::chrono::duration<double, _Unit>(m_worst).count();
    }

    /**
     * Enable output to an out stream
     */
    friend std::ostream &operator<<(std::ostream& os, const ProfileTimer& o) {
        return os <<
            o.m_name <<
            " counter: " << std::dec << o.m_counter <<
            ", total: " << o.GetTotalTime() <<
            " sec, avg: " << o.GetAvarageTime() <<
            " sec, best: " << o.GetBestTime() <<
            " sec, worst: " << o.GetWorstTime() <<
            " sec";
    }

private:
    const std::string m_name;
    uint64_t m_counter;
    std::chrono::high_resolution_clock::time_point m_enter;

    std::chrono::duration<uint64_t, std::nano> m_total;
    std::chrono::duration<uint64_t, std::nano> m_best;
    std::chrono::duration<uint64_t, std::nano> m_worst;
};

}
}

#endif //IBNET_SYS_PROFILETIMER_HPP
