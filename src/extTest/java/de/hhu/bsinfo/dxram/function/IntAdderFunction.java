package de.hhu.bsinfo.dxram.function;

import java.util.stream.IntStream;

import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class IntAdderFunction implements DistributableFunction<IntAdderFunction.Input, IntAdderFunction.Output> {

    public static final String NAME = "de.hhu.bsinfo.dxram.intadder";

    @Override
    public Output execute(ServiceProvider p_serviceAccessor, Input p_input) {
        int sum = IntStream.of(p_input.get()).sum();
        return new Output(sum);
    }

    public static final class Input implements Distributable {

        private int[] m_input;

        public Input() {}

        public Input(int... p_input) {
            m_input = p_input;
        }

        public int[] get() {
            return m_input;
        }

        @Override
        public void exportObject(Exporter p_exporter) {
            p_exporter.writeIntArray(m_input);
        }

        @Override
        public void importObject(Importer p_importer) {
            m_input = p_importer.readIntArray(m_input);
        }

        @Override
        public int sizeofObject() {
            return ObjectSizeUtil.sizeofIntArray(m_input);
        }
    }

    public static final class Output implements Distributable {

        private int m_output;

        public Output() {}

        public Output(int p_output) {
            m_output = p_output;
        }

        public int get() {
            return m_output;
        }

        @Override
        public void exportObject(Exporter p_exporter) {
            p_exporter.writeInt(m_output);
        }

        @Override
        public void importObject(Importer p_importer) {
            m_output = p_importer.readInt(m_output);
        }

        @Override
        public int sizeofObject() {
            return Integer.BYTES;
        }
    }
}
