
package de.hhu.bsinfo.dxram.log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.log.storage.LogCatalog;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.VersionsHashTable;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.utils.Tools;

/**
 * Reorganization thread
 *
 * @author Kevin Beineke 20.06.2014
 */
public final class SecondaryLogsReorgThread extends Thread {

	// Attributes
	private LogService m_logService;
	private LoggerComponent m_logger;
	private long m_secondaryLogSize;

	private boolean m_shutdown;

	private VersionsHashTable m_allVersions;

	private ReentrantLock m_reorganizationLock;
	private Condition m_reorganizationFinishedCondition;

	private final LinkedHashSet<SecondaryLog> m_reorganizationRequests;
	private ReentrantLock m_requestLock;

	private SecondaryLog m_secLog;
	private byte[] m_reorgSegmentData;
	private byte m_counter;
	private boolean m_isRandomChoice;

	// Constructors

	/**
	 * Creates an instance of SecondaryLogsReorgThread
	 *
	 * @param p_logService       the log service
	 * @param p_logger           the logger component
	 * @param p_secondaryLogSize the secondary log size
	 * @param p_logSegmentSize   the segment size
	 */
	public SecondaryLogsReorgThread(final LogService p_logService, final LoggerComponent p_logger,
			final long p_secondaryLogSize, final int p_logSegmentSize) {
		m_logService = p_logService;
		m_logger = p_logger;
		m_secondaryLogSize = p_secondaryLogSize;

		m_allVersions = new VersionsHashTable((int) (m_secondaryLogSize / 75 * 0.75), m_logger);

		m_reorganizationLock = new ReentrantLock(false);
		m_reorganizationFinishedCondition = m_reorganizationLock.newCondition();

		m_reorganizationRequests = new LinkedHashSet<SecondaryLog>();
		m_requestLock = new ReentrantLock(false);

		m_reorgSegmentData = new byte[p_logSegmentSize];

		m_counter = 0;
	}

	/**
	 * Shutdown
	 */
	public void shutdown() {
		m_shutdown = true;
	}

	// Setter

	/**
	 * Sets the secondary log to reorganize next
	 *
	 * @param p_secLog the Secondary Log
	 * @param p_await  whether to wait for completion of the reorganization or not
	 * @throws InterruptedException if caller is interrupted
	 */
	public void setLogToReorgImmediately(final SecondaryLog p_secLog, final boolean p_await)
			throws InterruptedException {

		if (p_await) {
			while (!m_reorganizationLock.tryLock()) {
				// Grant access for reorganization thread to avoid deadlock
				m_logService.grantReorgThreadAccessToCurrentLog();
			}
			m_secLog = p_secLog;
			while (m_secLog == p_secLog) {
				if (!m_reorganizationFinishedCondition.await(10, TimeUnit.MICROSECONDS)) {
					// Grant access for reorganization thread to avoid deadlock
					m_logService.grantReorgThreadAccessToCurrentLog();
				}
			}

			m_reorganizationLock.unlock();
		} else {
			m_requestLock.lock();
			m_reorganizationRequests.add(p_secLog);
			m_requestLock.unlock();
		}
	}

	@Override
	public void run() {
		final int iterationsPerLog = 20;
		int counter = 0;
		// long start;
		// long mid;
		Iterator<SecondaryLog> iter;
		SecondaryLog secondaryLog = null;

		while (!m_shutdown) {
			m_reorganizationLock.lock();
			// Check if there is an urgent reorganization request -> reorganize complete secondary log and signal
			if (m_secLog != null) {
				// #if LOGGER >= DEBUG
				m_logger.debug(SecondaryLogsReorgThread.class,
						"Got urgent reorganization request for " + m_secLog.getRangeIDOrFirstLocalID() + ".");
				// #endif /* LOGGER >= DEBUG */

				// Leave current secondary log
				if (counter > 0) {
					secondaryLog.resetReorgSegment();
					m_logService.leaveSecLog(secondaryLog);
					m_allVersions.clear();
					counter = 0;
				}

				// Reorganize complete secondary log
				secondaryLog = m_secLog;

				m_logService.getAccessToSecLog(secondaryLog);

				secondaryLog.getCurrentVersions(m_allVersions);
				secondaryLog.reorganizeAll(m_reorgSegmentData, m_allVersions);
				secondaryLog.resetReorgSegment();
				m_logService.leaveSecLog(secondaryLog);
				m_allVersions.clear();

				m_secLog = null;
				m_reorganizationFinishedCondition.signal();
				m_reorganizationLock.unlock();
				continue;
			}
			m_reorganizationLock.unlock();

			// Check if there are normal reorganization requests -> reorganize complete secondary logs
			m_requestLock.lock();
			if (!m_reorganizationRequests.isEmpty()) {
				m_requestLock.unlock();
				// Leave current secondary log
				if (counter > 0) {
					secondaryLog.resetReorgSegment();
					m_logService.leaveSecLog(secondaryLog);
					m_allVersions.clear();
					counter = 0;
				}

				// Process all reorganization requests
				while (!m_reorganizationRequests.isEmpty() && !m_shutdown) {
					m_reorganizationLock.lock();
					if (m_secLog != null) {
						// Favor urgent request
						m_reorganizationLock.unlock();
						break;
					}
					m_reorganizationLock.unlock();

					m_requestLock.lock();
					iter = m_reorganizationRequests.iterator();
					secondaryLog = iter.next();
					iter.remove();
					// #if LOGGER == TRACE
					m_logger.trace(SecondaryLogsReorgThread.class,
							"Got reorganization request for " + secondaryLog.getRangeIDOrFirstLocalID()
									+ ". Queue length: " + m_reorganizationRequests.size());
					// #endif /* LOGGER == TRACE */
					m_requestLock.unlock();

					// Reorganize complete secondary log
					m_logService.getAccessToSecLog(secondaryLog);
					secondaryLog.getCurrentVersions(m_allVersions);
					secondaryLog.reorganizeAll(m_reorgSegmentData, m_allVersions);
					secondaryLog.resetReorgSegment();
					m_logService.leaveSecLog(secondaryLog);
					m_allVersions.clear();
				}
				continue;
			} else {
				m_requestLock.unlock();
			}

			if (counter == 0) {
				// This is the first iteration -> choose secondary log and gather versions
				secondaryLog = chooseLog();
				if (null != secondaryLog && (secondaryLog.getOccupiedSpace() > m_secondaryLogSize / 2
						|| m_isRandomChoice)) {
					m_logService.getAccessToSecLog(secondaryLog);
					secondaryLog.getCurrentVersions(m_allVersions);
				} else {
					// Nothing to do -> wait for a while to reduce cpu load
					try {
						Thread.sleep(100);
					} catch (final InterruptedException e) {
					}
					continue;
				}
			}

			// Reorganize one segment
			// #if LOGGER == TRACE
			m_logger.trace(SecondaryLogsReorgThread.class,
					"Going to reorganize " + secondaryLog.getRangeIDOrFirstLocalID() + ".");
			// #endif /* LOGGER == TRACE */
			m_logService.getAccessToSecLog(secondaryLog);
			final long start = System.currentTimeMillis();
			secondaryLog.reorganizeIteratively(m_reorgSegmentData, m_allVersions);
			// #if LOGGER == TRACE
			m_logger.trace(SecondaryLogsReorgThread.class,
					"Time to reorganize segment: " + (System.currentTimeMillis() - start));
			// #endif /* LOGGER == TRACE */

			if (counter++ == iterationsPerLog) {
				// This was the last iteration for current secondary log -> clean-up
				secondaryLog.resetReorgSegment();
				m_logService.leaveSecLog(secondaryLog);
				m_allVersions.clear();
				counter = 0;
			}
		}
	}

	// Methods

	/**
	 * Determines next log to process
	 *
	 * @return secondary log
	 */
	private SecondaryLog chooseLog() {
		SecondaryLog ret = null;
		int numberOfLogs = 0;
		long max = 0;
		long current;
		LogCatalog cat;
		LogCatalog[] allCats;
		ArrayList<LogCatalog> cats;
		SecondaryLog[] secLogs;
		SecondaryLog secLog;

		allCats = m_logService.getAllLogCatalogs();
		cats = new ArrayList<LogCatalog>();
		for (int i = 0; i < allCats.length; i++) {
			cat = allCats[i];
			if (cat != null) {
				cats.add(cat);
			}
		}

		for (LogCatalog ct : cats) {
			numberOfLogs += ct.getNumberOfLogs();
		}

		/*
		 * Choose the largest log (or a log that has unreorganized segments within an advanced eon)
		 * To avoid starvation choose every third log randomly
		 */
		if (m_counter++ < 2) {
			m_isRandomChoice = false;
			outerloop:
			for (LogCatalog currentCat : cats) {
				secLogs = currentCat.getAllLogs();
				for (int j = 0; j < secLogs.length; j++) {
					secLog = secLogs[j];
					if (secLog != null) {
						if (secLog.needToBeReorganized()) {
							ret = secLog;
							break outerloop;
						}
						current = secLog.getOccupiedSpace();
						if (current > max) {
							max = current;
							ret = secLog;
						}
					}
				}
			}
		} else {
			m_counter = 0;
		}
		if (ret == null && !cats.isEmpty() && numberOfLogs > 1) {
			m_isRandomChoice = true;
			// Choose one secondary log randomly
			cat = cats.get(Tools.getRandomValue(cats.size() - 1));
			secLogs = cat.getAllLogs();
			if (secLogs.length > 0) {
				// Skip last log to speed up loading phase
				ret = secLogs[Tools.getRandomValue(secLogs.length - 2)];
			}
		}

		return ret;
	}
}
