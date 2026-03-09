package br.com.bellube.fastchannel.service.nativeapi;

import br.com.sankhya.jape.util.JapeSessionContext;

import javax.naming.InitialContext;
import javax.rmi.PortableRemoteObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bridge unificado para invocacao nativa de servicos Sankhya.
 *
 * Ordem de tentativas:
 * 1) ServiceInvoker legado (quando disponivel no runtime)
 * 2) modelcore.servicecaller.ServiceCaller (API nativa oficial)
 */
public class SankhyaNativeServiceCaller {

    private static final Logger log = Logger.getLogger(SankhyaNativeServiceCaller.class.getName());

    private static final String[] LEGACY_INVOKER_CLASS_CANDIDATES = {
            "br.com.sankhya.extensions.actionbutton.utils.ServiceInvoker",
            "br.com.sankhya.extensions.actionbutton.ServiceInvoker"
    };

    private static final String[] MODULE_CANDIDATES = {"mgecom", "mge"};
    private static final String MODELCORE_SERVICE_CALLER_CLASS = "br.com.sankhya.modelcore.servicecaller.ServiceCaller";
    private static final String MODELCORE_SERVICE_RESULT_CLASS = "br.com.sankhya.modelcore.servicecaller.ServiceCaller$ServiceResult";
    private static final String MGE_FRONT_FACADE_CLASS = "br.com.sankhya.modelcore.facades.MGEFrontFacade";
    private static final String MGE_FRONT_FACADE_HOME_CLASS = "br.com.sankhya.modelcore.facades.MGEFrontFacadeHome";
    private static final String SERVICE_CONTEXT_CLASS = "br.com.sankhya.ws.ServiceContext";

    public boolean isAvailable() {
        return hasLegacyInvoker() || isClassPresent(MODELCORE_SERVICE_CALLER_CLASS);
    }

    public String invoke(String serviceName, String requestXml, String sankhyaUser, String sankhyaPassword) throws Exception {
        List<String> errors = new ArrayList<>();

        try {
            return invokeViaLegacyServiceInvoker(serviceName, requestXml);
        } catch (Exception e) {
            errors.add("LegacyServiceInvoker: " + e.getMessage());
            log.log(Level.FINE, "Legacy ServiceInvoker indisponivel/falhou. Tentando ServiceCaller.", e);
        }

        try {
            return invokeViaModelcoreServiceCaller(serviceName, requestXml, sankhyaUser, sankhyaPassword);
        } catch (Exception e) {
            errors.add("ModelcoreServiceCaller: " + e.getMessage());
            log.log(Level.FINE, "Modelcore ServiceCaller indisponivel/falhou.", e);
        }

        throw new Exception("Falha na invocacao nativa de " + serviceName + ": " + String.join(" | ", errors));
    }

    private String invokeViaLegacyServiceInvoker(String serviceName, String requestXml) throws Exception {
        Class<?> invokerClass = resolveLegacyInvokerClass();
        Object invoker = createLegacyInvokerInstance(invokerClass, serviceName, requestXml);
        return invokeLegacyExecutor(invokerClass, invoker);
    }

    private String invokeViaModelcoreServiceCaller(String serviceName, String requestXml,
                                                   String sankhyaUser, String sankhyaPassword) throws Exception {
        Class<?> callerClass = Class.forName(MODELCORE_SERVICE_CALLER_CLASS);
        Class<?> serviceResultClass = Class.forName(MODELCORE_SERVICE_RESULT_CLASS);
        Class<?> frontFacadeClass = Class.forName(MGE_FRONT_FACADE_CLASS);

        Object frontFacade = resolveFrontFacade(frontFacadeClass, sankhyaUser, sankhyaPassword);
        if (frontFacade == null) {
            throw new Exception("MGEFrontFacade nao disponivel para ServiceCaller");
        }

        Constructor<?> ctor = callerClass.getConstructor(frontFacadeClass);
        Object caller = ctor.newInstance(frontFacade);
        Method callAsXml = callerClass.getMethod("callAsXml", String.class, String.class, String.class, serviceResultClass);

        String requestBody = extractRequestBodyContent(requestXml);
        InvocationState state = new InvocationState();
        Object callback = buildServiceResultCallback(serviceResultClass, state);

        StringBuilder tried = new StringBuilder();
        for (String module : MODULE_CANDIDATES) {
            state.reset();
            try {
                callAsXml.invoke(caller, serviceName, module, requestBody, callback);
            } catch (InvocationTargetException ite) {
                Throwable target = ite.getTargetException() != null ? ite.getTargetException() : ite;
                tried.append(module).append(": ").append(target.getMessage()).append(" | ");
                continue;
            }

            if (state.successPayload != null && !state.successPayload.trim().isEmpty()) {
                return state.successPayload;
            }

            if (state.failureMessage != null && !state.failureMessage.trim().isEmpty()) {
                tried.append(module).append(": ").append(state.failureMessage).append(" | ");
            } else {
                tried.append(module).append(": resposta vazia | ");
            }
        }

        throw new Exception("ServiceCaller nao retornou sucesso. Tentativas: " + tried);
    }

    private Class<?> resolveLegacyInvokerClass() throws ClassNotFoundException {
        for (String className : LEGACY_INVOKER_CLASS_CANDIDATES) {
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException ignored) {
            }
        }
        throw new ClassNotFoundException("ServiceInvoker legado nao encontrado");
    }

    private Object createLegacyInvokerInstance(Class<?> invokerClass, String serviceName, String requestXml) throws Exception {
        Exception lastError = null;

        try {
            Constructor<?> ctor = invokerClass.getConstructor(String.class, String.class);
            return ctor.newInstance(serviceName, requestXml);
        } catch (Exception e) {
            lastError = e;
        }

        try {
            Constructor<?> ctor = invokerClass.getConstructor(String.class);
            Object invoker = ctor.newInstance(serviceName);
            if (!setPayloadIfSupported(invokerClass, invoker, requestXml)) {
                throw new Exception("ServiceInvoker(String) sem metodo para payload");
            }
            return invoker;
        } catch (Exception e) {
            lastError = e;
        }

        try {
            Constructor<?> ctor = invokerClass.getConstructor();
            Object invoker = ctor.newInstance();
            if (!setServiceNameIfSupported(invokerClass, invoker, serviceName)) {
                throw new Exception("ServiceInvoker() sem metodo para serviceName");
            }
            if (!setPayloadIfSupported(invokerClass, invoker, requestXml)) {
                throw new Exception("ServiceInvoker() sem metodo para payload");
            }
            return invoker;
        } catch (Exception e) {
            lastError = e;
        }

        throw new Exception("Nao foi possivel instanciar ServiceInvoker legado", lastError);
    }

    private String invokeLegacyExecutor(Class<?> invokerClass, Object invoker) throws Exception {
        String[] execCandidates = {"invoke", "execute", "run"};
        Exception lastError = null;
        for (String methodName : execCandidates) {
            try {
                Method m = invokerClass.getMethod(methodName);
                Object response = m.invoke(invoker);
                return response == null ? null : response.toString();
            } catch (NoSuchMethodException e) {
                lastError = e;
            } catch (Exception e) {
                lastError = e;
                throw e;
            }
        }
        throw new Exception("ServiceInvoker sem metodo executavel conhecido", lastError);
    }

    private boolean setServiceNameIfSupported(Class<?> invokerClass, Object invoker, String serviceName) {
        String[] candidates = {"setServiceName", "setServico", "setService"};
        for (String methodName : candidates) {
            try {
                Method m = invokerClass.getMethod(methodName, String.class);
                m.invoke(invoker, serviceName);
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private boolean setPayloadIfSupported(Class<?> invokerClass, Object invoker, String requestXml) {
        String[] candidates = {"setRequestXml", "setRequestXML", "setRequestBody", "setXml", "setServiceRequest"};
        for (String methodName : candidates) {
            try {
                Method m = invokerClass.getMethod(methodName, String.class);
                m.invoke(invoker, requestXml);
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private Object resolveFrontFacade(Class<?> frontFacadeClass, String sankhyaUser, String sankhyaPassword) {
        Object fromServiceContext = resolveFrontFacadeFromServiceContext();
        if (fromServiceContext != null && frontFacadeClass.isInstance(fromServiceContext)) {
            return fromServiceContext;
        }

        Object fromJapeContext = JapeSessionContext.getProperty("MGEFrontFacade");
        if (fromJapeContext != null && frontFacadeClass.isInstance(fromJapeContext)) {
            return fromJapeContext;
        }

        Object fromJapeLower = JapeSessionContext.getProperty("mgeFrontFacade");
        if (fromJapeLower != null && frontFacadeClass.isInstance(fromJapeLower)) {
            return fromJapeLower;
        }

        return createFrontFacadeViaJndi(frontFacadeClass, sankhyaUser, sankhyaPassword);
    }

    private Object resolveFrontFacadeFromServiceContext() {
        try {
            Class<?> serviceContextClass = Class.forName(SERVICE_CONTEXT_CLASS);
            Object currentCtx = serviceContextClass.getMethod("getCurrent").invoke(null);
            if (currentCtx == null) return null;
            Object request = currentCtx.getClass().getMethod("getHttpRequest").invoke(currentCtx);
            if (request == null) return null;
            return request.getClass().getMethod("getAttribute", String.class).invoke(request, MGE_FRONT_FACADE_CLASS);
        } catch (Throwable e) {
            return null;
        }
    }

    private Object createFrontFacadeViaJndi(Class<?> frontFacadeClass, String sankhyaUser, String sankhyaPassword) {
        if (sankhyaUser == null || sankhyaUser.trim().isEmpty()) {
            return null;
        }
        if (sankhyaPassword == null) {
            sankhyaPassword = "";
        }
        try {
            Class<?> homeClass = Class.forName(MGE_FRONT_FACADE_HOME_CLASS);
            String jndiName = (String) homeClass.getField("JNDI_NAME").get(null);
            InitialContext ic = new InitialContext();
            Object homeRef = ic.lookup(jndiName);
            Object home = homeClass.isInstance(homeRef) ? homeRef : PortableRemoteObject.narrow(homeRef, homeClass);
            Method createMethod = homeClass.getMethod("create",
                    String.class, String.class, String.class, String.class, boolean.class, String.class);
            Object facade = createMethod.invoke(home, sankhyaUser, sankhyaPassword,
                    "FastchannelAddon", "127.0.0.1", false, "Fastchannel Native ServiceCaller");
            return frontFacadeClass.isInstance(facade) ? facade : null;
        } catch (Throwable e) {
            log.log(Level.FINE, "Nao foi possivel criar MGEFrontFacade via JNDI", e);
            return null;
        }
    }

    private Object buildServiceResultCallback(Class<?> serviceResultClass, InvocationState state) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("onSuccess".equals(method.getName())) {
                state.successPayload = args != null && args.length > 0 && args[0] != null ? args[0].toString() : null;
                state.failureMessage = null;
                return null;
            }
            if ("onFailure".equals(method.getName())) {
                String code = args != null && args.length > 0 && args[0] != null ? args[0].toString() : "0";
                String msg = args != null && args.length > 1 && args[1] != null ? args[1].toString() : "erro sem mensagem";
                state.successPayload = null;
                state.failureMessage = "status=" + code + " msg=" + msg;
                return null;
            }
            return null;
        };
        return Proxy.newProxyInstance(serviceResultClass.getClassLoader(), new Class[]{serviceResultClass}, handler);
    }

    private String extractRequestBodyContent(String requestXml) {
        if (requestXml == null) {
            return "";
        }
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?is)<requestBody>(.*)</requestBody>");
        java.util.regex.Matcher matcher = pattern.matcher(requestXml);
        if (matcher.find()) {
            String body = matcher.group(1);
            return body != null ? body.trim() : "";
        }
        return requestXml;
    }

    private boolean hasLegacyInvoker() {
        try {
            resolveLegacyInvokerClass();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isClassPresent(String fqcn) {
        try {
            Class.forName(fqcn);
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    private static final class InvocationState {
        private String successPayload;
        private String failureMessage;

        private void reset() {
            this.successPayload = null;
            this.failureMessage = null;
        }
    }
}

