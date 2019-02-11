package de.hhu.bsinfo.dxram.function.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import de.hhu.bsinfo.dxram.function.DistributableFunction;
import de.hhu.bsinfo.dxram.job.JobExecutable;

public class FunctionSerializer {

    public static byte[] serialize(final DistributableFunction p_function) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(p_function);
            return  bos.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

    public static DistributableFunction deserialize(final byte[] p_bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(p_bytes); ObjectInput in = new ObjectInputStream(bis)) {
            return (DistributableFunction) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

}
