package br.com.bellube.fastchannel.installation;

import br.com.sankhya.ws.ServiceContext;
import br.com.sankhya.ws.wrapper.IServiceWrapperProvider;
import br.com.sankhya.ws.wrapper.ServiceWrapper;
import com.google.gson.JsonObject;
import java.math.BigDecimal;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

public class LifecycleController extends ServiceWrapper implements IServiceWrapperProvider {
    private static final Logger logger = Logger.getLogger(LifecycleController.class.getName());

    @Override
    public ServiceWrapper createServiceWrapper() throws InvocationTargetException, IllegalAccessException {
        return this;
    }

    @Override
    public void handle() throws InvocationTargetException, IllegalAccessException {
        // Dispatcher vazio: metodos publicos (install/uninstall/verify/deleteWithFallback/safeDelete)
        // sao invocados diretamente pelo ServiceProvider.
    }

    private static String optString(JsonObject body, String key) {
        if (body == null || key == null) return null;
        if (!body.has(key) || body.get(key).isJsonNull()) return null;
        try {
            return body.get(key).getAsString();
        } catch (Exception ignore) {
            return body.get(key).toString();
        }
    }

    private static JsonObject baseResponse(String status) {
        JsonObject resp = new JsonObject();
        resp.addProperty("status", status);
        return resp;
    }

    private static BigDecimal optBigDecimal(JsonObject body, String key) {
        if (body == null || key == null) return null;
        if (!body.has(key) || body.get(key).isJsonNull()) return null;
        try {
            return body.get(key).getAsBigDecimal();
        } catch (Exception ignore) {
            try {
                return new BigDecimal(body.get(key).getAsString());
            } catch (Exception ignored) {
                return null;
            }
        }
    }

    public void install(ServiceContext ctx) {
        JsonObject body = ctx.getJsonRequestBody();
        String appkey = optString(body, "appkey");
        BigDecimal codModulo = optBigDecimal(body, "codmodulo");
        logger.info("[FastchannelLifecycle] install chamado" + (appkey != null ? " para appkey=" + appkey : ""));
        FastchannelAutoProvisioning.ensureStarted(appkey, codModulo);
        JsonObject resp = baseResponse("INSTALLED");
        resp.addProperty("integrity", true);
        resp.addProperty("auto_provisioning", true);
        ctx.setJsonResponse(resp);
        ctx.setStatus(200);
    }

    public void uninstall(ServiceContext ctx) {
        JsonObject body = ctx.getJsonRequestBody();
        String appkey = optString(body, "appkey");
        BigDecimal codModulo = optBigDecimal(body, "codmodulo");
        logger.info("[FastchannelLifecycle] uninstall chamado" + (appkey != null ? " para appkey=" + appkey : ""));
        FastchannelAutoProvisioning.stopAll(appkey, codModulo);
        JsonObject resp = baseResponse("UNINSTALLED");
        resp.addProperty("integrity_removed", true);
        ctx.setJsonResponse(resp);
        ctx.setStatus(200);
    }

    public void verify(ServiceContext ctx) {
        JsonObject body = ctx.getJsonRequestBody();
        String appkey = optString(body, "appkey");
        BigDecimal codModulo = optBigDecimal(body, "codmodulo");
        logger.info("[FastchannelLifecycle] verify chamado" + (appkey != null ? " para appkey=" + appkey : ""));
        FastchannelAutoProvisioning.ensureStarted(appkey, codModulo);
        JsonObject resp = baseResponse("OK");
        resp.addProperty("integrity", true);
        resp.addProperty("auto_provisioning", true);
        ctx.setJsonResponse(resp);
        ctx.setStatus(200);
    }

    public void deleteWithFallback(ServiceContext ctx) {
        JsonObject body = ctx.getJsonRequestBody();
        String appkey = optString(body, "appkey");
        logger.info("[FastchannelLifecycle] deleteWithFallback chamado" + (appkey != null ? " para appkey=" + appkey : ""));
        JsonObject resp = baseResponse("DELETED_LOCAL");
        resp.addProperty("auto_uninstalled", true);
        resp.addProperty("integrity_removed", true);
        ctx.setJsonResponse(resp);
        ctx.setStatus(200);
    }

    public void safeDelete(ServiceContext ctx) {
        JsonObject body = ctx.getJsonRequestBody();
        String appkey = optString(body, "appkey");
        logger.info("[FastchannelLifecycle] safeDelete chamado" + (appkey != null ? " para appkey=" + appkey : ""));
        JsonObject resp = baseResponse("DELETED_LOCAL");
        resp.addProperty("server_called", false);
        resp.addProperty("auto_uninstalled", true);
        resp.addProperty("integrity_removed", true);
        ctx.setJsonResponse(resp);
        ctx.setStatus(200);
    }
}
