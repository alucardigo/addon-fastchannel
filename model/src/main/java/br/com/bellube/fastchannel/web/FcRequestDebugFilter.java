package br.com.bellube.fastchannel.web;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.logging.Logger;
import br.com.sankhya.ws.ServiceContext;

public class FcRequestDebugFilter implements Filter {
    private static final Logger log = Logger.getLogger(FcRequestDebugFilter.class.getName());

    @Override
    public void init(FilterConfig filterConfig) {
        // no-op
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest req = (HttpServletRequest) request;
            String uri = req.getRequestURI();
            if (uri != null && uri.contains("addon-fastchannel")) {
                String qs = req.getQueryString();
                String mgeSession = req.getParameter("mgeSession");
                String jsession = null;
                Cookie[] cookies = req.getCookies();
                if (cookies != null) {
                    for (Cookie c : cookies) {
                        if ("JSESSIONID".equalsIgnoreCase(c.getName())) {
                            jsession = c.getValue();
                            break;
                        }
                    }
                }
                String scSession = null;
                try {
                    ServiceContext sc = ServiceContext.getCurrent();
                    if (sc != null) {
                        scSession = sc.getHttpSessionId();
                    }
                } catch (Throwable t) {
                    scSession = "error";
                }
                log.info("[fc-debug] uri=" + uri
                        + (qs == null ? "" : ("?"+qs))
                        + " mgeSession=" + (mgeSession == null ? "<null>" : mgeSession)
                        + " JSESSIONID=" + (jsession == null ? "<null>" : jsession)
                        + " ServiceContext=" + (scSession == null ? "<null>" : scSession));
            }
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // no-op
    }
}
