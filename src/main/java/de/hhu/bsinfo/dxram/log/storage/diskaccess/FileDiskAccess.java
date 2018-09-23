package de.hhu.bsinfo.dxram.log.storage.diskaccess;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;

/**
 * Disk access using a RandomAccessFile.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class FileDiskAccess implements DiskAccessInterface {

    @Override
    public long getFileSize(final Object p_log) throws IOException {
        return ((RandomAccessFile) p_log).length();
    }

    @Override
    public Object createLog(final File p_file, final long p_logSize) throws IOException {
        boolean success = true;

        if (p_file.exists()) {
            success = p_file.delete();
        }

        if (success && !p_file.getParentFile().exists()) {
            // Create folders
            success = p_file.getParentFile().mkdirs();
        }

        if (success) {
            // Create file
            success = p_file.createNewFile();

            if (success) {
                // Write header
                return new RandomAccessFile(p_file, "rw");
            }
        }

        return null;
    }

    @Override
    public Object openLog(final File p_file) throws IOException {
        return new RandomAccessFile(p_file, "r");
    }

    @Override
    public void renameLog(File p_file, File p_newFile) throws IOException {
        Files.move(p_file.toPath(), p_newFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void closeLog(final Object p_log) throws IOException {
        if (p_log != null) {
            ((RandomAccessFile) p_log).close();
        }
    }

    @Override
    public void closeAndRemoveLog(final Object p_log, final File p_file) throws IOException {
        ((RandomAccessFile) p_log).close();

        if (p_file.exists()) {
            if (!p_file.delete()) {
                throw new FileNotFoundException();
            }
        }
    }

    @Override
    public void read(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_length,
            final long p_readPos) throws IOException {
        ((RandomAccessFile) p_log).seek(p_readPos);
        ((RandomAccessFile) p_log).readFully(p_bufferWrapper.getBuffer().array(), 0, p_length);
    }

    @Override
    public void write(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length, final boolean p_setLength) throws IOException {
        ((RandomAccessFile) p_log).seek(p_writePos);
        ((RandomAccessFile) p_log).write(p_bufferWrapper.getBuffer().array(), p_bufferOffset, p_length);
        if (p_setLength) {
            ((RandomAccessFile) p_log).setLength(p_writePos + p_length);
        }
    }

    @Override
    public void append(final Object p_log, final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset,
            final long p_writePos, final int p_length) throws IOException {
        ((RandomAccessFile) p_log).seek(p_writePos);
        ((RandomAccessFile) p_log).write(p_bufferWrapper.getBuffer().array(), p_bufferOffset, p_length);
        ((RandomAccessFile) p_log).setLength(p_writePos + p_length);
    }
}
