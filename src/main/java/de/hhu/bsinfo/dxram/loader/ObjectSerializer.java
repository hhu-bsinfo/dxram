package de.hhu.bsinfo.dxram.loader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ObjectSerializer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ObjectSerializer.class);

    private ObjectSerializer() {
    }

    public static byte[] serializeObject(Object p_map) {
        byte[] yourBytes = null;

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(p_map);
            out.flush();
            yourBytes = bos.toByteArray();
        } catch (IOException e) {
            LOGGER.error(e);
        }

        return yourBytes;
    }

    public static <T> T deserializeObject(byte[] p_bytes, Class<T> p_type) {
        T object = null;
        try (ByteArrayInputStream fis = new ByteArrayInputStream(p_bytes)) {
            ObjectInputStream ois = new ObjectInputStream(fis);
            object = (T) ois.readObject();
            ois.close();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error(e);
        }

        return object;
    }
}
