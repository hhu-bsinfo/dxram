
package de.hhu.bsinfo.dxram.test;

import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import de.hhu.bsinfo.utils.Contract;
import de.hhu.bsinfo.utils.Tools;

/**
 * Controls the execution of a test case
 * @author Florian Klein
 *         13.02.2014
 */
public final class TestCase {

	// Constants
	private static final String FILENAME_TEMP_OUT = "tc.temp";

	// Attributes
	private AbstractInput m_input;

	private TestResult m_result;

	private boolean m_oppressSystemOuts;

	// Constructors
	/**
	 * Creates an instance of TestCase
	 * @param p_input
	 *            the input
	 */
	public TestCase(final AbstractInput p_input) {
		m_input = p_input;

		m_result = null;

		m_oppressSystemOuts = false;
	}

	// Setters
	/**
	 * If true the SystemOuts during the test case execution is oppressed
	 * @param p_oppress
	 *            true if SystemOuts should be oppressed
	 */
	public void setOppressSystemOuts(final boolean p_oppress) {
		m_oppressSystemOuts = p_oppress;
	}

	// Methods
	/**
	 * Executes the test case
	 * @return the TestResult
	 * @throws TestCaseException
	 *             in case of an exception during (de-)initialization or with the input
	 */
	public TestResult execute() throws TestCaseException {
		return execute(false);
	}

	/**
	 * Executes the test case
	 * @param p_abortOnException
	 *            if true the test case is aborted when an exception occurrs
	 * @return the TestResult
	 * @throws TestCaseException
	 *             in case of an exception during (de-)initialization or with the input
	 */
	public TestResult execute(final boolean p_abortOnException) throws TestCaseException {
		TestResult ret;
		boolean success = true;
		AbstractOperation operation;
		TestContext context;

		PrintStream out = null;
		File file = null;

		if (m_oppressSystemOuts) {
			out = System.out;
			try {
				file = new File(FILENAME_TEMP_OUT);
				file.deleteOnExit();
				System.setOut(new PrintStream(file));
			} catch (final FileNotFoundException e) {}
		}

		context = new TestContext();
		m_result = new TestResult(context);

		if (m_input != null) {

			if (m_input.getInit() != null) {
				try {
					if (!m_input.getInit().execute(context)) {
						throw new TestCaseException("Initializiation failed");
					}
				} catch (final Exception e) {
					throw new TestCaseException("Exception during initializiation", e);
				}
			}

			m_input.prepareGet();
			m_result.setStartTime(System.currentTimeMillis());
			operation = m_input.getNext();
			while (operation != null) {
				try {
					success &= operation.execute(context);
				} catch (final Exception e) {
					if (p_abortOnException) {
						throw new TestCaseException("Test aborted on exception", new OperationException(operation, e));
					}

					m_result.addException(new OperationException(operation, e));

					success = false;
				}

				operation = m_input.getNext();
			}
			m_result.setEndTime(System.currentTimeMillis());
			m_input.finishGet();

			if (m_input.getDeinit() != null) {
				try {
					if (!m_input.getDeinit().execute(context)) {
						throw new TestCaseException("Deinitializiation failed");
					}
				} catch (final Exception e) {
					throw new TestCaseException("Exception during deinitializiation", e);
				}
			}
		}

		m_result.setSuccess(success);

		if (m_oppressSystemOuts) {
			System.out.close();
			System.setOut(out);
		}

		ret = m_result;

		return ret;
	}

	/**
	 * Executes the test case multiple times
	 * @param p_times
	 *            the number of executions
	 * @return the TestResult
	 * @throws TestCaseException
	 *             in case of an exception during (de-)initialization or with the input
	 */
	public List<TestResult> executeMultiple(final int p_times) throws TestCaseException {
		return executeMultiple(p_times, false);
	}

	/**
	 * Executes the test case multiple times
	 * @param p_times
	 *            the number of executions
	 * @param p_abortOnException
	 *            if true the test case is aborted when an exception occurrs
	 * @return the TestResult
	 * @throws TestCaseException
	 *             in case of an exception during (de-)initialization or with the input
	 */
	public List<TestResult> executeMultiple(final int p_times, final boolean p_abortOnException) throws TestCaseException {
		List<TestResult> ret;
		int div;

		div = Math.max(p_times / 10, 1);

		// TODO: true
		m_oppressSystemOuts = false;

		System.out.print("0%");

		ret = new ArrayList<>();
		for (int i = 1; i <= p_times; i++) {
			ret.add(execute(p_abortOnException));

			if (i % div == 0) {
				System.out.print(" - " + i * 100 / p_times + "%");
			}

			System.gc();
		}
		System.out.println();

		return ret;
	}

	/**
	 * Gets the TestResult with the minimum time
	 * @param p_results
	 *            the test results
	 * @return the TestResult with the minimum time
	 */
	public static TestResult getMinimumResult(final List<TestResult> p_results) {
		TestResult ret;

		Contract.checkNotNull(p_results, "no results given");

		ret = null;
		for (TestResult result : p_results) {
			if (ret == null || result.getTime() < ret.getTime()) {
				ret = result;
			}
		}

		return ret;
	}

	/**
	 * Gets the TestResult with the maximum time
	 * @param p_results
	 *            the test results
	 * @return the TestResult with the maximum time
	 */
	public static TestResult getMaximumResult(final List<TestResult> p_results) {
		TestResult ret;

		Contract.checkNotNull(p_results, "no results given");

		ret = null;
		for (TestResult result : p_results) {
			if (ret == null || result.getTime() > ret.getTime()) {
				ret = result;
			}
		}

		return ret;
	}

	/**
	 * Gets the minimum time of the test results
	 * @param p_results
	 *            the test results
	 * @return the minimum time
	 */
	public static long getMinimumTime(final List<TestResult> p_results) {
		return getMinimumResult(p_results).getTime();
	}

	/**
	 * Gets the maximum time of the test results
	 * @param p_results
	 *            the test results
	 * @return the maximum time
	 */
	public static long getMaximumTime(final List<TestResult> p_results) {
		return getMaximumResult(p_results).getTime();
	}

	/**
	 * Gets the average time of the test results
	 * @param p_results
	 *            the test results
	 * @return the average time
	 */
	public static long getAverageTime(final List<TestResult> p_results) {
		long ret;

		Contract.checkNotNull(p_results, "no results given");

		ret = 0;
		for (TestResult result : p_results) {
			ret += result.getTime();
		}
		ret /= p_results.size();

		return ret;
	}

	/**
	 * Prints the status of the test results
	 * @param p_results
	 *            the test results
	 */
	public static void printStatus(final List<TestResult> p_results) {
		printStatus(p_results, new DefaultEvaluation(), false);
	}

	/**
	 * Prints the status of the test results
	 * @param p_results
	 *            the test results
	 * @param p_evaluation
	 *            hte evaluation
	 */
	public static void printStatus(final List<TestResult> p_results, final TestResultEvaluation p_evaluation) {
		printStatus(p_results, p_evaluation, false);
	}

	/**
	 * Prints the status of the test results
	 * @param p_results
	 *            the test results
	 * @param p_evaluation
	 *            hte evaluation
	 * @param p_details
	 *            if true every test is listed
	 */
	public static void printStatus(final List<TestResult> p_results, final TestResultEvaluation p_evaluation, final boolean p_details) {
		int i;
		StringBuilder out;
		Throwable e;

		Contract.checkNotNull(p_results, "no results given");

		out = new StringBuilder();
		out.append("---\n");

		if (p_details) {
			i = 1;
			for (TestResult result : p_results) {
				out.append("Test (" + i + "/" + p_results.size() + ")\n");
				if (result.existException()) {
					out.append(result.getExceptions().size() + " exception(s) occured\n");

					do {
						e = result.getExceptions().get(0).getCause();
						out.append(e.getClass().getSimpleName() + ": " + e.getMessage() + "\n");
						for (StackTraceElement element : e.getStackTrace()) {
							out.append("\tat " + element.getClassName() + "." + element.getMethodName() + "(" + element.getFileName() + ":"
									+ element.getLineNumber() + ")\n");
						}
						e = e.getCause();
						if (e != null) {
							out.append("\nCaused by:\n");
						}
					} while (e != null);
				} else {
					p_evaluation.evaluate(result);
					out.append(p_evaluation.getDetails() + "\n");
				}

				i++;
			}

			out.append("---\n");
		} else {
			i = 0;
			for (TestResult result : p_results) {
				p_evaluation.evaluate(result);

				if (result.isSuccess()) {
					i++;
				}
			}

			out.append(i + " out of " + p_results.size() + " tests succeeded\n");
			out.append("---\n");
		}

		if (p_evaluation.printTimes()) {
			out.append("Minimum Time: " + Tools.readableTime(TestCase.getMinimumTime(p_results)) + "\n");
			out.append("Maximum Time: " + Tools.readableTime(TestCase.getMaximumTime(p_results)) + "\n");
			out.append("Average Time: " + Tools.readableTime(TestCase.getAverageTime(p_results)) + "\n");
			out.append("---\n");
		}

		System.out.println(out);
	}

	/**
	 * Creates a test case input where all operations are kept in the memory
	 * @return a test case input
	 */
	public static AbstractInput createMemoryInput() {
		return new MemoryInput();
	}

	/**
	 * Creates a test case input where all operations are kept in the memory
	 * @param p_init
	 *            the init operation
	 * @param p_deinit
	 *            the deinit operation
	 * @param p_operations
	 *            the operations
	 * @return a test case input
	 */
	public static AbstractInput createMemoryInput(final AbstractOperation p_init, final AbstractOperation p_deinit, final AbstractOperation... p_operations) {
		AbstractInput ret;

		ret = new MemoryInput();

		ret.setInit(p_init);
		ret.setDeinit(p_deinit);

		ret.prepareAdd();
		for (int i = 0; i < p_operations.length; i++) {
			ret.add(p_operations[i]);
		}
		ret.finishAdd();

		return ret;
	}

	/**
	 * Creates a test case input where all operations are kept in a file
	 * @param p_prefix
	 *            the prefix of the test files
	 * @return a test case input
	 */
	public static AbstractInput createFileInput(final String p_prefix) {
		return new FileInput(p_prefix);
	}

	/**
	 * Creates a test case input where all operations are kept in a file
	 * @param p_prefix
	 *            the prefix of the test files
	 * @param p_init
	 *            the init operation
	 * @param p_deinit
	 *            the deinit operation
	 * @param p_operations
	 *            the operations
	 * @return a test case input
	 */
	public static AbstractInput createMemoryInput(final String p_prefix, final AbstractOperation p_init, final AbstractOperation p_deinit,
			final AbstractOperation... p_operations) {
		AbstractInput ret;

		ret = new FileInput(p_prefix);

		ret.setInit(p_init);
		ret.setDeinit(p_deinit);

		ret.prepareAdd();
		for (int i = 0; i < p_operations.length; i++) {
			ret.add(p_operations[i]);
		}
		ret.finishAdd();

		return ret;
	}

	// Classes
	/**
	 * Represents a test result
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public final class TestResult {

		// Attributes
		private boolean m_success;

		private List<OperationException> m_exceptions;

		private long m_startTime;
		private long m_endTime;
		private List<InterimTime> m_interimTimes;

		private TestContext m_context;

		// Constructors
		/**
		 * Creates an instance of TestResult
		 * @param p_context
		 *            the TestContext
		 */
		private TestResult(final TestContext p_context) {
			m_success = false;

			m_startTime = 0;
			m_endTime = 0;

			m_exceptions = new ArrayList<>();
			m_interimTimes = new ArrayList<>();

			m_context = p_context;
		}

		// Getters
		/**
		 * Return if the test was successful
		 * @return true if the test was successful, false otherwise
		 */
		public boolean isSuccess() {
			return m_success;
		}

		/**
		 * Gets the TestContext
		 * @return the TestContext
		 */
		public TestContext getContext() {
			return m_context;
		}

		// Setters
		/**
		 * Sets if the test was successful
		 * @param p_success
		 *            true if the test was successful, false otherwise
		 */
		private void setSuccess(final boolean p_success) {
			m_success = p_success;
		}

		/**
		 * Sets the start time
		 * @param p_startTime
		 *            the start time
		 */
		private void setStartTime(final long p_startTime) {
			m_startTime = p_startTime;
		}

		/**
		 * Sets the end time
		 * @param p_endTime
		 *            the end time
		 */
		private void setEndTime(final long p_endTime) {
			m_endTime = p_endTime;
		}

		// Methods
		/**
		 * Adds an exception
		 * @param p_exception
		 *            the exception
		 */
		private void addException(final OperationException p_exception) {
			m_exceptions.add(p_exception);
		}

		/**
		 * Adds an interim time
		 * @param p_time
		 *            the interim time
		 */
		private void addInterimTime(final InterimTime p_time) {
			m_interimTimes.add(p_time);
		}

		/**
		 * Checks if an exception exists
		 * @return true if an exception exist, false otherwise
		 */
		public boolean existException() {
			boolean ret = false;

			ret = m_exceptions.size() > 0;

			return ret;
		}

		/**
		 * Return the occurred exceptions
		 * @return the occurred exceptions
		 */
		public List<OperationException> getExceptions() {
			List<OperationException> ret = null;

			ret = m_exceptions;

			return ret;
		}

		/**
		 * Gets the time of the test case execution
		 * @return the time of the test case execution
		 */
		public long getTime() {
			return m_endTime - m_startTime;
		}

		/**
		 * Prints the time of the test case execution to System.out
		 */
		public void printTime() {
			printTime(false);
		}

		/**
		 * Prints the time of the test case execution to System.out
		 * @param p_interimTimes
		 *            if true, the interim times will be printed too
		 */
		public void printTime(final boolean p_interimTimes) {
			StringBuilder builder;

			builder = new StringBuilder();
			builder.append("TestCase finished in ");
			builder.append(Tools.readableTime(m_endTime - m_startTime));

			if (p_interimTimes) {
				if (m_interimTimes.isEmpty()) {
					builder.append("\nno interim times!");
				} else {
					for (InterimTime time : m_interimTimes) {
						builder.append("\n");
						builder.append(time.getTitle());
						builder.append(":\t\t");
						builder.append(Tools.readableTime(time.getTime() - m_startTime));
					}
				}
			}

			System.out.println(builder.toString());
		}

	}

	/**
	 * Calls dynamically an method of an object instance or class
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public static final class MethodCall extends AbstractOperation {

		// Attributes
		private Object m_instance;
		private String m_class;
		private String m_method;
		private boolean m_staticMethod;

		private Object[] m_arguments;

		// Constructors
		/**
		 * Creates an instance of MethodCall
		 * @param p_instance
		 *            the object instance
		 * @param p_method
		 *            the method
		 * @param p_arguments
		 *            the arguments
		 */
		public MethodCall(final Object p_instance, final String p_method, final Object... p_arguments) {
			Contract.checkNotNull(p_instance, "no class instance given");
			Contract.checkNotNull(p_method, "no methodname given");

			m_instance = p_instance;
			m_class = null;
			m_method = p_method;
			m_staticMethod = false;

			m_arguments = p_arguments;
		}

		/**
		 * Creates an instance of MethodCallOperation
		 * @param p_class
		 *            the class
		 * @param p_method
		 *            the method
		 * @param p_arguments
		 *            the arguments
		 */
		public MethodCall(final String p_class, final String p_method, final Object... p_arguments) {
			Contract.checkNotNull(p_class, "no classname given");
			Contract.checkNotNull(p_method, "no methodname given");

			m_instance = null;
			m_class = p_class;
			m_method = p_method;
			m_staticMethod = true;

			m_arguments = p_arguments;
		}

		// Methods
		@Override
		public boolean execute(final TestContext p_context) throws ReflectiveOperationException {
			Class<?> clazz;
			Method method;

			if (m_staticMethod) {
				clazz = Class.forName(m_class);
			} else {
				clazz = m_instance.getClass();
			}
			method = clazz.getDeclaredMethod(m_method, getArgumentClasses());
			method.setAccessible(true);
			method.invoke(m_instance, m_arguments);

			return true;
		}

		/**
		 * Gets the argument classes
		 * @return the argument classes
		 */
		private Class<?>[] getArgumentClasses() {
			Class<?>[] ret = null;

			if (m_arguments != null) {
				ret = new Class<?>[m_arguments.length];
				for (int i = 0; i < m_arguments.length; i++) {
					ret[i] = m_arguments[i].getClass();
				}
			}

			return ret;
		}

	}

	/**
	 * Bundles multiple operations
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public static final class OperationBundle extends AbstractOperation {

		// Attributes
		private ArrayList<AbstractOperation> m_operations;

		// Constructors
		/**
		 * Creates an instance of OperationBundle
		 */
		public OperationBundle() {
			m_operations = new ArrayList<>();
		}

		// Methods
		/**
		 * Adds an operation
		 * @param p_operation
		 *            the operation to add
		 */
		public void add(final AbstractOperation p_operation) {
			m_operations.add(p_operation);
		}

		@Override
		public boolean execute(final TestContext p_context) throws IOException, ReflectiveOperationException {
			boolean ret = true;

			for (AbstractOperation operation : m_operations) {
				ret &= operation.execute(p_context);
			}

			return ret;
		}

		@Override
		public void writeExternal(final ObjectOutput p_output) throws IOException {
			p_output.writeInt(m_operations.size());
			for (AbstractOperation operation : m_operations) {
				p_output.writeUTF(operation.getClass().getName());
				operation.writeExternal(p_output);
			}
		}

		@Override
		public void readExternal(final ObjectInput p_input) throws IOException {
			int count;
			AbstractOperation operation;

			count = p_input.readInt();
			for (int i = 0; i < count; i++) {
				try {
					operation = AbstractOperation.class.cast(Class.forName(p_input.readUTF()).newInstance());
					operation.readExternal(p_input);
					m_operations.add(operation);
				} catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {}
			}
		}
	}

	/**
	 * Represents an executable operation
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public abstract static class AbstractOperation implements Externalizable {

		// Constructors
		/**
		 * Creates an instance of AbstractOperation
		 */
		public AbstractOperation() {}

		// Methods
		/**
		 * Executes the operation
		 * @param p_context
		 *            the context of the test
		 * @return true if the operation succeeds, false otherwise
		 * @throws IOException
		 *             if execution failed
		 * @throws ReflectiveOperationException
		 *             if execution failed
		 */
		public abstract boolean execute(TestContext p_context) throws IOException, ReflectiveOperationException;

		@Override
		public void writeExternal(final ObjectOutput p_output) throws IOException {}

		@Override
		public void readExternal(final ObjectInput p_input) throws IOException {}

	}

	/**
	 * A test case input where all operations are kept in the memory
	 * @author Florian Klein
	 *         13.02.2014
	 */
	private static class MemoryInput extends AbstractInput {

		// Attributes
		private AbstractOperation m_init;
		private AbstractOperation m_deinit;
		private List<AbstractOperation> m_operations;
		private Iterator<AbstractOperation> m_iterator;

		// Constructors
		/**
		 * Creates an instance of MemoryInput
		 */
		MemoryInput() {
			m_init = null;
			m_deinit = null;
			m_operations = null;
			m_iterator = null;
		}

		// Methods
		@Override
		protected AbstractOperation doGetInit() {
			return m_init;
		}

		@Override
		protected AbstractOperation doGetDeinit() {
			return m_deinit;
		}

		@Override
		protected void doSetInit(final AbstractOperation p_init) {
			m_init = p_init;
		}

		@Override
		protected void doSetDeinit(final AbstractOperation p_deinit) {
			m_deinit = p_deinit;
		}

		@Override
		protected void doPrepareAdd() {
			m_operations = new ArrayList<>();
		}

		@Override
		protected void doFinishAdd() {}

		@Override
		protected void doAdd(final AbstractOperation p_operation) {
			if (m_operations != null) {
				m_operations.add(p_operation);
			}
		}

		@Override
		protected void doPrepareGet() {
			if (m_iterator == null && m_operations != null) {
				m_iterator = m_operations.iterator();
			}
		}

		@Override
		protected void doFinishGet() {
			m_iterator = null;
		}

		@Override
		protected AbstractOperation doGetNext() {
			AbstractOperation ret = null;

			if (m_iterator != null && m_iterator.hasNext()) {
				ret = m_iterator.next();
			}

			return ret;
		}

	}

	/**
	 * A test case input where all operations are kept in a file
	 * @author Florian Klein
	 *         13.02.2014
	 */
	private static class FileInput extends AbstractInput {

		// Constants
		private static final String TEST_CASE_EXTENSION = ".tc";
		private static final String INIT_OPERATION_EXTENSION = ".itc";
		private static final String DEINIT_OPERATION_EXTENSION = ".dtc";

		// Attributes
		private String m_prefix;
		private ObjectInput m_input;
		private ObjectOutput m_output;

		// Constructors
		/**
		 * Creates an instance of FileInput
		 * @param p_prefix
		 *            the prefix of the test files
		 */
		FileInput(final String p_prefix) {
			m_prefix = p_prefix;

			m_input = null;
			m_output = null;
		}

		// Methods
		@Override
		protected AbstractOperation doGetInit() throws IOException, ReflectiveOperationException {
			AbstractOperation ret = null;
			File file;
			ObjectInput input;

			file = new File(m_prefix + INIT_OPERATION_EXTENSION);
			if (file.exists()) {
				input = new ObjectInputStream(new FileInputStream(file));
				ret = AbstractOperation.class.cast(Class.forName(input.readUTF()).newInstance());
				ret.readExternal(input);
				input.close();
			}

			return ret;
		}

		@Override
		protected AbstractOperation doGetDeinit() throws IOException, ReflectiveOperationException {
			AbstractOperation ret = null;
			File file;
			ObjectInput input;

			file = new File(m_prefix + DEINIT_OPERATION_EXTENSION);
			if (file.exists()) {
				input = new ObjectInputStream(new FileInputStream(file));
				ret = AbstractOperation.class.cast(Class.forName(input.readUTF()).newInstance());
				ret.readExternal(input);
				input.close();
			}

			return ret;
		}

		@Override
		protected void doSetInit(final AbstractOperation p_init) throws IOException {
			File file;
			ObjectOutput output;

			file = new File(m_prefix + INIT_OPERATION_EXTENSION);
			if (file.exists()) {
				file.delete();
			}

			if (p_init != null) {
				output = new ObjectOutputStream(new FileOutputStream(file));
				output.writeUTF(p_init.getClass().getName());
				p_init.writeExternal(output);
				output.close();
			}
		}

		@Override
		protected void doSetDeinit(final AbstractOperation p_deinit) throws IOException {
			File file;
			ObjectOutput output;

			file = new File(m_prefix + DEINIT_OPERATION_EXTENSION);
			if (file.exists()) {
				file.delete();
			}

			if (p_deinit != null) {
				output = new ObjectOutputStream(new FileOutputStream(file));
				output.writeUTF(p_deinit.getClass().getName());
				p_deinit.writeExternal(output);
				output.close();
			}
		}

		@Override
		public void doPrepareAdd() throws IOException {
			File file;

			file = new File(m_prefix + TEST_CASE_EXTENSION);
			if (file.exists()) {
				file.delete();
			}

			m_output = new ObjectOutputStream(new FileOutputStream(file));
		}

		@Override
		public void doFinishAdd() throws IOException {
			if (m_output != null) {
				m_output.writeBoolean(false);
				m_output.close();
				m_output = null;
			}
		}

		@Override
		protected void doAdd(final AbstractOperation p_operation) throws IOException {
			if (m_output != null) {
				m_output.writeBoolean(true);
				m_output.writeUTF(p_operation.getClass().getName());
				p_operation.writeExternal(m_output);
			}
		}

		@Override
		public void doPrepareGet() throws IOException {
			m_input = new ObjectInputStream(new FileInputStream(new File(m_prefix + TEST_CASE_EXTENSION)));
		}

		@Override
		public void doFinishGet() throws IOException {
			if (m_input != null) {
				m_input.close();
				m_input = null;
			}
		}

		@Override
		protected AbstractOperation doGetNext() throws IOException, ReflectiveOperationException {
			AbstractOperation ret = null;

			if (m_input != null) {
				if (m_input.readBoolean()) {
					ret = AbstractOperation.class.cast(Class.forName(m_input.readUTF()).newInstance());
					ret.readExternal(m_input);
				} else {
					m_input.close();
					m_input = null;
				}
			}

			return ret;
		}

	}

	/**
	 * Represents a test case input
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public abstract static class AbstractInput {

		// Constructors
		/**
		 * Creates an instance of AbstractInput
		 */
		public AbstractInput() {}

		// Methods
		/**
		 * Gets the init operation
		 * @return the init operation
		 * @throws TestCaseException
		 *             if the init operation could not be get
		 */
		public final AbstractOperation getInit() throws TestCaseException {
			AbstractOperation ret = null;

			try {
				ret = doGetInit();
			} catch (final Exception e) {
				throw new TestCaseException("Could not get init operation", e);
			}

			return ret;
		}

		/**
		 * Gets the init operation
		 * @return the init operation
		 * @throws IOException
		 *             if the init operation could not be gotten
		 * @throws ReflectiveOperationException
		 *             if the init operation could not be gotten
		 */
		protected abstract AbstractOperation doGetInit() throws IOException, ReflectiveOperationException;

		/**
		 * Sets the init operation
		 * @param p_init
		 *            the init operation
		 * @throws TestCaseException
		 *             if the init operation could not be set
		 */
		public final void setInit(final AbstractOperation p_init) throws TestCaseException {
			try {
				doSetInit(p_init);
			} catch (final Exception e) {
				throw new TestCaseException("Could not set init operation", e);
			}
		}

		/**
		 * Sets the init operation
		 * @param p_init
		 *            the init operation
		 * @throws IOException
		 *             if the init operation could not be set
		 * @throws ReflectiveOperationException
		 *             if the init operation could not be set
		 */
		protected abstract void doSetInit(final AbstractOperation p_init) throws IOException, ReflectiveOperationException;

		/**
		 * Gets the deinit operation
		 * @return the deinit operation
		 * @throws TestCaseException
		 *             if the deinit operation could not be get
		 */
		public final AbstractOperation getDeinit() throws TestCaseException {
			AbstractOperation ret = null;

			try {
				ret = doGetDeinit();
			} catch (final Exception e) {
				throw new TestCaseException("Could not get deinit operation", e);
			}

			return ret;
		}

		/**
		 * Gets the deinit operation
		 * @return the deinit operation
		 * @throws IOException
		 *             if the deinit operation could not be gotten
		 * @throws ReflectiveOperationException
		 *             if the deinit operation could not be gotten
		 */
		protected abstract AbstractOperation doGetDeinit() throws IOException, ReflectiveOperationException;

		/**
		 * Sets the deinit operation
		 * @param p_deinit
		 *            the deinit operation
		 * @throws TestCaseException
		 *             if the deinit operation could not be set
		 */
		public final void setDeinit(final AbstractOperation p_deinit) throws TestCaseException {
			try {
				doSetDeinit(p_deinit);
			} catch (final Exception e) {
				throw new TestCaseException("Could not set deinit operation", e);
			}
		}

		/**
		 * Sets the deinit operation
		 * @param p_deinit
		 *            the deinit operation
		 * @throws IOException
		 *             if the deinit operation could not be set
		 */
		protected abstract void doSetDeinit(final AbstractOperation p_deinit) throws IOException;

		/**
		 * Prepares the input for adding operations
		 * @throws TestCaseException
		 *             if the preparation fails
		 */
		public final void prepareAdd() throws TestCaseException {
			try {
				doPrepareAdd();
			} catch (final Exception e) {
				throw new TestCaseException("Could not prepare add", e);
			}
		}

		/**
		 * Prepares the input for adding operations
		 * @throws IOException
		 *             if the preparation fails
		 */
		protected void doPrepareAdd() throws IOException {}

		/**
		 * Completes the adding of operations
		 * @throws TestCaseException
		 *             if the completion fails
		 */
		public final void finishAdd() throws TestCaseException {
			try {
				doFinishAdd();
			} catch (final Exception e) {
				throw new TestCaseException("Could not finish add", e);
			}
		}

		/**
		 * Completes the adding of operations
		 * @throws IOException
		 *             if the completion fails
		 */
		protected void doFinishAdd() throws IOException {}

		/**
		 * Adds an operation
		 * @param p_operation
		 *            the operation
		 * @throws TestCaseException
		 *             if the operation could not be added
		 */
		public final void add(final AbstractOperation p_operation) throws TestCaseException {
			Contract.checkNotNull(p_operation, "no operation given");

			try {
				doAdd(p_operation);
			} catch (final Exception e) {
				throw new TestCaseException("Could not add operation", e);
			}
		}

		/**
		 * Adds an operation
		 * @param p_operation
		 *            the operation
		 * @throws IOException
		 *             if the operation could not be added
		 */
		protected abstract void doAdd(final AbstractOperation p_operation) throws IOException;

		/**
		 * Prepares the input for getting operations
		 * @throws TestCaseException
		 *             if the preparation fails
		 */
		protected final void prepareGet() throws TestCaseException {
			try {
				doPrepareGet();
			} catch (final Exception e) {
				throw new TestCaseException("Could not prepare get", e);
			}
		}

		/**
		 * Prepares the input for getting operations
		 * @throws IOException
		 *             if the preparation fails
		 */
		protected void doPrepareGet() throws IOException {}

		/**
		 * Completes the getting of operations
		 * @throws TestCaseException
		 *             if the completion fails
		 */
		protected final void finishGet() throws TestCaseException {
			try {
				doFinishGet();
			} catch (final Exception e) {
				throw new TestCaseException("Could not finish get", e);
			}
		}

		/**
		 * Completes the getting of operations
		 * @throws IOException
		 *             if the completion fails
		 */
		protected void doFinishGet() throws IOException {}

		/**
		 * Gets the next operation
		 * @return the next operation
		 * @throws TestCaseException
		 *             if the next operation could not be get
		 */
		protected final AbstractOperation getNext() throws TestCaseException {
			AbstractOperation ret = null;

			try {
				ret = doGetNext();
			} catch (final Exception e) {
				throw new TestCaseException("Could not get operation", e);
			}

			return ret;
		}

		/**
		 * Gets the next operation
		 * @return the next operation
		 * @throws IOException
		 *             if the next operation could not be get
		 * @throws ReflectiveOperationException
		 *             if the next operation could not be get
		 */
		protected abstract AbstractOperation doGetNext() throws IOException, ReflectiveOperationException;

	}

	/**
	 * Stores data during the test case execution
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public final class TestContext {

		// Attributes
		private Map<String, Object> m_map;

		// Constructors
		/**
		 * Creates an instance of TestContext
		 */
		private TestContext() {
			m_map = new HashMap<>();
		}

		// Methods
		/**
		 * Gets the value for the given key
		 * @param p_key
		 *            the key
		 * @return the corresponfing value
		 */
		public Object get(final String p_key) {
			return m_map.get(p_key);
		}

		/**
		 * Gets the value for the given key
		 * @param p_key
		 *            the key
		 * @param p_type
		 *            the class of the value
		 * @param <Type>
		 *            the class of the value
		 * @return the corresponfing value
		 */
		public <Type> Type get(final String p_key, final Class<Type> p_type) {
			Type ret = null;
			Object o;

			o = m_map.get(p_key);
			if (o != null && p_type.isAssignableFrom(o.getClass())) {
				ret = p_type.cast(o);
			}

			return ret;
		}

		/**
		 * Sets the value for the given key
		 * @param p_key
		 *            the key
		 * @param p_value
		 *            the value
		 * @param <Type>
		 *            the class of the value
		 */
		public <Type> void set(final String p_key, final Type p_value) {
			m_map.put(p_key, p_value);
		}

		/**
		 * Removes the value for the given key
		 * @param p_key
		 *            the key
		 */
		public void remove(final String p_key) {
			m_map.remove(p_key);
		}

		/**
		 * Adds an interim time operation
		 */
		public void addInterimTime() {
			addInterimTime("");
		}

		/**
		 * Adds an interim time operation
		 * @param p_title
		 *            the titel for the interim time
		 */
		public void addInterimTime(final String p_title) {
			Contract.checkNotNull(p_title, "no title given");

			m_result.addInterimTime(new InterimTime(p_title, System.currentTimeMillis()));
		}

	}

	/**
	 * Evaluates a test result
	 * @author Florian Klein
	 *         02.04.2014
	 */
	public interface TestResultEvaluation {

		// Methods
		/**
		 * Evaluates the given test result
		 * @param p_result
		 *            the test result
		 */
		void evaluate(final TestResult p_result);

		/**
		 * Gets a string representation of the evaluation
		 * @return the string representation
		 */
		String getDetails();

		/**
		 * Checks if the times should be printed
		 * @return true if the time should be printed, false otherwise
		 */
		boolean printTimes();

	}

	/**
	 * Default evaluation
	 * @author Florian Klein
	 *         02.04.2014
	 */
	private static class DefaultEvaluation implements TestResultEvaluation {

		// Attributes
		private TestResult m_result;

		// Constructors
		/**
		 * Creates an instance of DefaultEvaluation
		 */
		DefaultEvaluation() {}

		@Override
		public void evaluate(final TestResult p_result) {
			m_result = p_result;
		}

		@Override
		public String getDetails() {
			StringBuilder out;

			Contract.checkNotNull(m_result, "no result given");

			out = new StringBuilder();
			out.append("---\n");
			if (m_result.isSuccess()) {
				out.append("succeeded\n");
			} else {
				out.append("failed\n");
			}
			out.append("Time: " + Tools.readableTime(m_result.getTime()) + "\n");
			if (m_result.existException()) {
				out.append(m_result.getExceptions().size() + " exceptions occured\n");
			}
			out.append("---\n");

			return out.toString();
		}

		@Override
		public boolean printTimes() {
			return true;
		}

	}

	/**
	 * Represents an interim time
	 * @author Florian Klein
	 *         13.02.2014
	 */
	private class InterimTime {

		// Attributes
		private String m_title;
		private long m_time;

		// Constructors
		/**
		 * Creates an instance of InterimTime
		 * @param p_title
		 *            the title for the interim time
		 * @param p_time
		 *            the interim time
		 */
		InterimTime(final String p_title, final long p_time) {
			m_title = p_title;
			m_time = p_time;
		}

		// Getters
		/**
		 * Gets the title
		 * @return the title
		 */
		public final String getTitle() {
			return m_title;
		}

		/**
		 * Gets the time
		 * @return the time
		 */
		public final long getTime() {
			return m_time;
		}

	}

	/**
	 * Wraps exceptions which occurr during a test case execution
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public static final class TestCaseException extends RuntimeException {

		// Constants
		private static final long serialVersionUID = -3329070128240541490L;

		// Constructors
		/**
		 * Creates an instance of TestCaseException
		 * @param p_message
		 *            the message
		 */
		private TestCaseException(final String p_message) {
			super(p_message);
		}

		/**
		 * Creates an instance of TestCaseException
		 * @param p_message
		 *            the message
		 * @param p_cause
		 *            the cause
		 */
		private TestCaseException(final String p_message, final Exception p_cause) {
			super(p_message, p_cause);
		}

	}

	/**
	 * Wraps exceptions which occurr during an operation execution
	 * @author Florian Klein
	 *         13.02.2014
	 */
	public final class OperationException extends Exception {

		// Constants
		private static final long serialVersionUID = -5732091856646983274L;

		// Attributes
		private final AbstractOperation m_operation;

		// Constructors
		/**
		 * Creates an instance of OperationException
		 * @param p_operation
		 *            the operation in which the exception occured
		 * @param p_exception
		 *            the occurred exception
		 */
		private OperationException(final AbstractOperation p_operation, final Exception p_exception) {
			super("Exception in " + p_operation.getClass().getSimpleName() + " " + p_operation, p_exception);

			m_operation = p_operation;
		}

		// Getters
		/**
		 * Gets the operation
		 * @return the operation
		 */
		public AbstractOperation getOperation() {
			return m_operation;
		}

	}

}
