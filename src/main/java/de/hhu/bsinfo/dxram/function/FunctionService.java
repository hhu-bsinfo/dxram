package de.hhu.bsinfo.dxram.function;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.function.messages.ExecuteFunctionMessage;
import de.hhu.bsinfo.dxram.function.messages.ExecuteFunctionRequest;
import de.hhu.bsinfo.dxram.function.messages.ExecuteFunctionResponse;
import de.hhu.bsinfo.dxram.function.messages.FunctionMessages;
import de.hhu.bsinfo.dxram.function.messages.RegisterFunctionRequest;
import de.hhu.bsinfo.dxram.function.messages.RegisterFunctionResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.serialization.Distributable;

@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class FunctionService extends Service<ModuleConfig> implements MessageReceiver {

    private final Map<String, DistributableFunction> m_functions = new ConcurrentHashMap<>();

    private NetworkComponent m_network;

    public enum Status {
        FAILED, REGISTERED
    }

    public Status register(final short p_nodeId, final String p_name, final DistributableFunction p_function) {
        RegisterFunctionRequest registerFunctionRequest = new RegisterFunctionRequest(p_nodeId, p_function, p_name);

        LOGGER.debug("Registering function %s on node %04X", p_name, p_nodeId);

        try {
            m_network.sendSync(registerFunctionRequest);
        } catch (NetworkException e) {
            LOGGER.warn("Couldn't send function registration %s to node %04X", p_name, p_nodeId);
            return Status.FAILED;
        }

        RegisterFunctionResponse response = registerFunctionRequest.getResponse(RegisterFunctionResponse.class);

        return response.isRegistered() ? Status.REGISTERED : Status.FAILED;
    }

    public Status register(final String p_name, final DistributableFunction p_function) {
        if (p_function == null) {
            LOGGER.warn("Registered function %s must not be null", p_name);
            return Status.FAILED;
        }

        LOGGER.debug("Registering function %s", p_name);

        m_functions.put(p_name, p_function);

        return Status.REGISTERED;
    }

    public Distributable execute(final String p_name, final Distributable p_input) {
        DistributableFunction function = m_functions.get(p_name);

        if (function == null) {
            LOGGER.warn("Trying to execute non-registered function %s", p_name);
            return null;
        }

        LOGGER.debug("Executing function %s", p_name);

        return function.execute(getParentEngine(), p_input);
    }

    public void execute(final short p_nodeId, final String p_name, final Distributable p_input) {
        ExecuteFunctionMessage executeFunctionMessage = new ExecuteFunctionMessage(p_nodeId, p_name, p_input);

        LOGGER.debug("Executing function %s on node %04X", p_name, p_nodeId);

        try {
            m_network.sendMessage(executeFunctionMessage);
        } catch (NetworkException e) {
            LOGGER.warn("Couldn't send function execution %s to node %04X", p_name, p_nodeId);
        }
    }

    public <T extends Distributable> T executeSync(final short p_nodeId, final String p_name, final Distributable p_input) {
        ExecuteFunctionRequest executeFunctionRequest = new ExecuteFunctionRequest(p_nodeId, p_name, p_input);

        LOGGER.debug("Executing function %s on node %04X", p_name, p_nodeId);

        try {
            m_network.sendSync(executeFunctionRequest);
        } catch (NetworkException e) {
            LOGGER.warn("Couldn't send function execution %s to node %04X", p_name, p_nodeId);
            return null;
        }

        ExecuteFunctionResponse response = executeFunctionRequest.getResponse(ExecuteFunctionResponse.class);

        if (response.hasResult()) {
            return (T) response.getResult();
        }

        return null;
    }

    @Override
    protected void resolveComponentDependencies(ComponentProvider p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        registerMessageTypes();
        registerMessageListeners();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    @Override
    public void onIncomingMessage(Message p_message) {
        switch (p_message.getSubtype()) {
            case FunctionMessages.SUBTYPE_REGISTER_FUNCTION_REQUEST:
                handle((RegisterFunctionRequest) p_message);
                break;
            case FunctionMessages.SUBTYPE_EXECUTE_FUNCTION_REQUEST:
                handle((ExecuteFunctionRequest) p_message);
                break;
            case FunctionMessages.SUBTYPE_EXECUTE_FUNCTION:
                handle((ExecuteFunctionMessage) p_message);
                break;
            default:
                break;
        }
    }

    private void handle(final RegisterFunctionRequest p_message) {
        Status status = register(p_message.getName(), p_message.getFunction());

        RegisterFunctionResponse response = new RegisterFunctionResponse(p_message, status == Status.REGISTERED);

        try {
            m_network.sendMessage(response);
        } catch (NetworkException e) {
            LOGGER.warn("Couldn't send registration response for function %s to node %04X", p_message.getName(), response.getDestination());
        }

    }

    private void handle(final ExecuteFunctionRequest p_message) {
        Distributable result = execute(p_message.getName(), p_message.getInput());

        ExecuteFunctionResponse response = new ExecuteFunctionResponse(p_message, result);

        try {
            m_network.sendMessage(response);
        } catch (NetworkException e) {
            LOGGER.warn("Couldn't send result for function %s to node %04X", p_message.getName(), response.getDestination());
        }
    }

    private void handle(final ExecuteFunctionMessage p_message) {
        execute(p_message.getName(), p_message.getInput());
    }

    private void registerMessageTypes() {
        m_network.registerMessageType(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_REGISTER_FUNCTION_REQUEST,
                RegisterFunctionRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_REGISTER_FUNCTION_RESPONSE,
                RegisterFunctionResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION_REQUEST,
                ExecuteFunctionRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION_RESPONSE,
                ExecuteFunctionResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION,
                ExecuteFunctionMessage.class);
    }

    private void registerMessageListeners() {
        m_network.register(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_REGISTER_FUNCTION_REQUEST, this);
        m_network.register(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION_REQUEST, this);
        m_network.register(DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION, this);
    }
}
