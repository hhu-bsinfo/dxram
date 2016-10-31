package de.hhu.bsinfo.utils.io;

/**
 * Common interface/class for a data provider.
 * The source is not known to the outside. It can be a file,
 * network socket or any other kind of stream.
 * Use this to implement different ways of providing
 * data.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 08.03.2016
 */
public abstract class AbstractIODataStream {
    /**
     * Possible operation modes for the stream.
     */
    public enum OperationMode {
        INVALID, READ, WRITE
    }

    /**
     * Error codes return by some functions.
     */
    public enum ErrorCode {
        SUCCESS, UNKNOWN, NOT_OPENED, ALREADY_OPENED, FILE_NOT_FOUND, ACCESS_DENIED,
    }

    private String m_type = null;
    private String m_address = null;
    private OperationMode m_mode = null;

    // --------------------------------------------------------------------------------------

    /**
     * Constructor
     *
     * @param p_type
     *         Type name for the stream.
     * @param p_address
     *         Address/File/Path...
     */
    public AbstractIODataStream(final String p_type, final String p_address) {
        m_type = p_type;
        m_address = p_address;
        m_mode = OperationMode.INVALID;
    }

    /**
     * "Destructor"
     * We want to make sure that everything gets cleaned up.
     * For this we have the <code>close()</code> call, but if the
     * user doesn't call/use it, we still want to make sure that
     * everything gets closed properly when the object gets destroyed.
     */
    @Override protected void finalize() {
        // make sure to cleanup if the user didn't, before object gets
        // cleaned up by the garbage collector
        if (m_mode != OperationMode.INVALID) {
            close();
        }
    }

    // --------------------------------------------------------------------------------------

    /**
     * Get the type string.
     *
     * @return Type string.
     */
    public String getType() {
        return m_type;
    }

    /**
     * Get the provided address of the stream.
     *
     * @return String holding the address of the stream.
     */
    public String getAddress() {
        return m_address;
    }

    /**
     * Get the operation mode.
     *
     * @return OperationMode.
     */
    public OperationMode getMode() {
        return m_mode;
    }

    /**
     * Get the provided operation mode of the stream.
     *
     * @return Operation mode of the stream.
     */
    public OperationMode getOperationMode() {
        return m_mode;
    }

    /**
     * Check if the stream is writable.
     *
     * @return True if writable, false if read only.
     */
    public boolean isWritable() {
        return m_mode == OperationMode.WRITE;
    }

    /**
     * Check if the stream is read only.
     *
     * @return True if read only, false if also writable.
     */
    public boolean isReadOnly() {
        return m_mode == OperationMode.READ;
    }

    /**
     * Set the operation mode.
     *
     * @param p_mode
     *         OperationMode.
     */
    public void setMode(final OperationMode p_mode) {
        m_mode = p_mode;
    }

    // --------------------------------------------------------------------------------------

    /**
     * Open the stream.
     * The stream pointer will point to the beginning (0) of the stream.
     *
     * @param p_mode
     *         Operation mode for the stream.
     * @param p_overwrite
     *         Overwrite any existing files/parameters appointed by the address.
     * @return ERROR_CODE Result of the operation.
     */
    public abstract ErrorCode open(OperationMode p_mode, boolean p_overwrite);

    /**
     * Close the stream.
     * Make sure to call this before the object gets destroyed to
     * execute proper closing and cleanup of the stream.
     *
     * @return ERROR_CODE Result of the operation.
     */
    public abstract ErrorCode close();

    /**
     * Check if the stream is opened.
     *
     * @return True if already opened and ready for operation, false otherwise
     */
    public abstract boolean isOpened();

    /**
     * Read a given amount of bytes from the stream.
     * This takes the current position of the stream pointer into
     * consideration. Use <code> seek(long pos)</code> to move
     * the pointer to a different location.
     *
     * @param p_data
     *         Pre-allocated buffer with a size of at least specified size.
     * @param p_offset
     *         Offset of the array to start reading from.
     * @param p_size
     *         Number of bytes to read.
     * @return Number of bytes read. Compare to check if you hit eof while reading (less bytes read than specified) or -1 on error.
     */
    public abstract int read(byte[] p_data, int p_offset, int p_size);

    /**
     * Same as the other read call, but read size is the size of the array and offset 0.
     *
     * @param p_data
     *         Preallocated buffer for reading.
     * @return Number of bytes read. Compare to check if you hit eof while reading (less bytes read than specified) or -1 on error.
     */
    public int read(final byte[] p_data) {
        return read(p_data, 0, p_data.length);
    }

    /**
     * Write a buffer to the byte stream.
     * This takes the current position of the stream pointer in
     * consideration. Use <code> seek(long pos)</code> to move
     * the pointer to a different location.
     *
     * @param p_data
     *         Data to be written.
     * @param p_offset
     *         Offset of the array to start writing to.
     * @param p_size
     *         Number of bytes of the array to be written.
     * @return Number of bytes written or -1 on error.
     */
    public abstract int write(byte[] p_data, int p_offset, int p_size);

    /**
     * Same as the other write call, but write size is the size of the array and offset 0.
     *
     * @param p_data
     *         Preallocated buffer for writing.
     * @return Number of bytes written or -1 on error.
     */
    public int write(final byte[] p_data) {
        return write(p_data, 0, p_data.length);
    }

    /**
     * Seek to a position within the stream.
     * Seeking beyond EOF (size) should not be possible.
     *
     * @param p_pos
     *         Absolute position to seek to in bytes.
     * @return True if successful, false on error.
     */
    public abstract boolean seek(long p_pos);

    /**
     * Check if EOF is currently hit.
     *
     * @return True if EOF is hit, false otherwise.
     */
    public abstract boolean eof();

    /**
     * Tell the current position of the internal pointer.
     *
     * @return Current position of the internal pointer in bytes or -1 on error.
     */
    public abstract long tell();

    /**
     * Get the total size of the stream.
     *
     * @return Size of the stream in bytes or -1 on error.
     */
    public abstract long size();

    /**
     * Flush any unwritten data.
     * A function which isn't needed for every implementation.
     *
     * @return True if flushing was sucessful, false otherwise.
     */
    public abstract boolean flush();
}
