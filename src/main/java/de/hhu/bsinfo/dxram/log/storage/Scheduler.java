package de.hhu.bsinfo.dxram.log.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.logs.LogHandler;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.writebuffer.WriteBufferHandler;

/**
 * This class allows thread signaling across different packages (e.g., the exclusive message handler blocks the
 * reorganization thread during recovery).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.09.2018
 */
public final class Scheduler {

    private static final Logger LOGGER = LogManager.getFormatterLogger(Scheduler.class.getSimpleName());

    private WriteBufferHandler m_writeBufferHandler;
    private LogHandler m_logHandler;

    /**
     * Creates an instance of Scheduler.
     */
    public Scheduler() {
    }

    /**
     * Sets the handler. Cannot be set in constructor because of cyclic dependencies.
     *
     * @param p_writeBufferHandler
     *         the write buffer handler to flush the write buffer
     * @param p_logHandler
     *         the log handler to signal, block and grant access to the reorganization thread
     */
    public void set(final WriteBufferHandler p_writeBufferHandler, final LogHandler p_logHandler) {
        m_writeBufferHandler = p_writeBufferHandler;
        m_logHandler = p_logHandler;
    }

    /**
     * Flushes the write buffer.
     */
    public void flushWriteBuffer() {
        m_writeBufferHandler.flushWriteBuffer();
    }

    /**
     * Flushes the write buffer. Waits for a specific range to be flushed.
     *
     * @param p_owner
     *         the owner
     * @param p_range
     *         the range
     */
    public void flushWriteBuffer(final short p_owner, final short p_range) {
        m_writeBufferHandler.flushWriteBuffer(p_owner, p_range);
    }

    /**
     * Wakes up the reorganization thread.
     * Must not be called by any other thread than the writer thread.
     *
     * @param p_secondaryLog
     *         the secondary log to reorganize
     */
    public void signalReorganization(final SecondaryLog p_secondaryLog) {
        m_logHandler.signalReorganization(p_secondaryLog);
    }

    /**
     * Wakes up the reorganization thread and waits until reorganization is finished.
     * Must not be called by any other thread than the writer thread.
     *
     * @param p_secondaryLog
     *         the secondary log to reorganize
     */
    public void signalReorganizationBlocking(final SecondaryLog p_secondaryLog) {
        m_logHandler.signalReorganizationBlocking(p_secondaryLog);
    }

    /**
     * Grants the reorganization thread access to a log.
     * Is called by process thread, only.
     */
    public void grantAccessToReorganization() {
        if (m_logHandler == null) {
            LOGGER.error("Not yet initialized!");
            return;
        }

        m_logHandler.grantAccessToReorganization();
    }

    /**
     * Blocks the reorganization thread.
     */
    public void blockReorganizationThread() {
        m_logHandler.blockReorganizationThread();

    }

    /**
     * Unblocks the reorganization thread.
     */
    public void unblockReorganizationThread() {
        m_logHandler.unblockReorganizationThread();
    }

}
