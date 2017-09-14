#include "IbConnectionManagerJobQueue.h"

#include <thread>

namespace ibnet {
namespace core {

IbConnectionManagerJobQueue::IbConnectionManagerJobQueue(uint32_t size) :
    m_size(size),
    m_front(0),
    m_back(0),
    m_backRes(0),
    m_queue(new Job[size])
{

}

IbConnectionManagerJobQueue::~IbConnectionManagerJobQueue(void)
{
    delete m_queue;
}

bool IbConnectionManagerJobQueue::PushBack(const Job& job)
{
    uint32_t backRes = m_backRes.load(std::memory_order_relaxed);
    uint32_t front;

    while (true) {
        front = m_front.load(std::memory_order_relaxed);

        if (backRes + 1 % m_size == front % m_size) {
            return false;
        }

        if (m_backRes.compare_exchange_weak(backRes, backRes + 1,
            std::memory_order_relaxed)) {
            m_queue[backRes % m_size] = job;

            // wait for any preceding reservations to complete before updating
            // back
            while (!m_back.compare_exchange_weak(backRes, backRes + 1,
                std::memory_order_release)) {
                std::this_thread::yield();
            }

            return true;
        }
    }
}

bool IbConnectionManagerJobQueue::PopFront(Job& job)
{
    uint32_t front = m_front.load(std::memory_order_relaxed);
    uint32_t back = m_back.load(std::memory_order_relaxed);

    if (front % m_size == back % m_size) {
        return false;
    }

    job = m_queue[front % m_size];

    m_front.fetch_add(1, std::memory_order_release);
    return true;
}

bool IbConnectionManagerJobQueue::IsEmpty(void) const
{
    uint32_t front = m_front.load(std::memory_order_relaxed);
    uint32_t back = m_back.load(std::memory_order_relaxed);

    return front % m_size == back % m_size;
}

}
}