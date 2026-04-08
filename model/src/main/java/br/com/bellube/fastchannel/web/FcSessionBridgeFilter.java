package br.com.bellube.fastchannel.web;

import br.com.sankhya.ws.ServiceContext;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Ensures the request carries JSESSIONID matching mgeSession param
 * so DefaultMgeFilter can find the existing session.
 */
public class FcSessionBridgeFilter implements Filter {
    private static final Logger log = Logger.getLogger(FcSessionBridgeFilter.class.getName());
    @Override
    public void init(FilterConfig filterConfig) {
        // no-op
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest)) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest req = (HttpServletRequest) request;
        String mgeSession = req.getParameter("mgeSession");
        if (mgeSession == null || mgeSession.trim().isEmpty()) {
            chain.doFilter(request, response);
            return;
        }

        // Ensure ServiceContext exists for downstream filters
        try {
            if (ServiceContext.getCurrent() == null) {
                ServiceContext sc = new ServiceContext(req);
                sc.makeCurrent();
            }
        } catch (Throwable t) {
            // ignore, fallback to default behavior
        }

        // Ensure ModuleMgr has an MGESession for the provided mgeSession (via reflection)
        try {
            Class<?> moduleMgrClass = Class.forName("br.com.sankhya.modulemgr.ModuleMgr");
            Object moduleMgr = moduleMgrClass.getMethod("getSingleton").invoke(null);
            if (moduleMgr != null) {
                Object existing = moduleMgrClass.getMethod("getMgeSession", String.class)
                        .invoke(moduleMgr, mgeSession);
                if (existing == null) {
                    Class<?> mgeSessionClass = Class.forName("br.com.sankhya.modulemgr.MGESession");
                    Object mgeSess = mgeSessionClass
                            .getConstructor(javax.servlet.http.HttpSession.class)
                            .newInstance(req.getSession());
                    mgeSessionClass.getMethod("setMgeSessionId", String.class)
                            .invoke(mgeSess, mgeSession);
                    moduleMgrClass.getMethod("addMgeSessionOnCache", String.class, mgeSessionClass)
                            .invoke(moduleMgr, mgeSession, mgeSess);
                    log.info("[fc-session] ModuleMgr session cached for " + mgeSession);
                } else {
                    log.info("[fc-session] ModuleMgr session already present for " + mgeSession);
                }
            }
        } catch (Throwable t) {
            log.log(Level.WARNING, "[fc-session] ModuleMgr integration failed: " + t.getMessage(), t);
        }

        // Do NOT alter cookies/headers. Only ensure ModuleMgr has the session mapped.
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // no-op
    }
}
