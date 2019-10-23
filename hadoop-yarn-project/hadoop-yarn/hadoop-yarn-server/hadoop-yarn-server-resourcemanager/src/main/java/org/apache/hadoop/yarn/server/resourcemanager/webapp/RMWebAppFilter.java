/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

@Singleton
public class RMWebAppFilter extends GuiceContainer {

	private Injector injector;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// define a set of URIs which do not need to do redirection
	private static final Set<String> NON_REDIRECTED_URIS = Sets.newHashSet("/conf", "/stacks", "/logLevel", "/metrics",
			"/jmx", "/logs");
	private String path;
	private static final int BASIC_SLEEP_TIME = 5;
	private static final int MAX_SLEEP_TIME = 5 * 60;
	private static final Random randnum = new Random();
	private static final Log LOG = LogFactory.getLog(RMWebAppFilter.class.getName());
	private boolean basicAuthEnabled = false;// 是否开启basic auth
	private String basicAuth = "";// 认证信息
	
	@Inject
	public RMWebAppFilter(Injector injector, Configuration conf) {
		super(injector);
		this.injector = injector;
		InetSocketAddress sock = YarnConfiguration.useHttps(conf)
				? conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
						YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
						YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT)
				: conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
						YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);

		path = sock.getHostName() + ":" + Integer.toString(sock.getPort());
		path = YarnConfiguration.useHttps(conf) ? "https://" + path : "http://" + path;
		basicAuthEnabled = conf.getBoolean("http.basic.auth.enabled", false);// yxd
		basicAuth = conf.get("http.basic.auth");// yxd
	}

	// yxd
	private boolean checkHeaderAuth(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String auth = request.getHeader("Authorization");
		if ((auth != null) && (auth.length() > 6)) {
			auth = auth.substring(6, auth.length());
			String decodedAuth = getFromBASE64(auth);
//			System.out.println("auth decoded from base64 is " + decodedAuth);
			request.getSession().setAttribute("auth", decodedAuth);
			if (basicAuth != null && decodedAuth.equals(basicAuth)) {
				request.getSession().setMaxInactiveInterval(604800);// yxd 设置成1星期
				return true;
			}
		}
		return false;
	}

	// yxd
	private String getFromBASE64(String s) {
		if (s == null)
			return null;
		Base64 decoder = new Base64();
		try {
			byte[] b = decoder.decode(s);
			return new String(b);
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		if (basicAuthEnabled && !IPTools.isInner(request.getRemoteAddr())) {// 外网才需要认证, 避免CDH监控失败
			if (!checkHeaderAuth(request, response)) {
				response.setStatus(401);
				response.setHeader("Cache-Control", "no-store");
				response.setDateHeader("Expires", 0);
				response.setHeader("WWW-authenticate", "Basic Realm=\"\"");
				return;
			}
		}

		response.setCharacterEncoding("UTF-8");
		String htmlEscapedUri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());

		if (htmlEscapedUri == null) {
			htmlEscapedUri = "/";
		}

		String uriWithQueryString = WebAppUtils.appendQueryParams(request, htmlEscapedUri);
		String htmlEscapedUriWithQueryString = WebAppUtils.getHtmlEscapedURIWithQueryString(request);

		RMWebApp rmWebApp = injector.getInstance(RMWebApp.class);
		rmWebApp.checkIfStandbyRM();
		if (rmWebApp.isStandby() && shouldRedirect(rmWebApp, htmlEscapedUri)) {

			String redirectPath = rmWebApp.getRedirectPath();

			if (redirectPath != null && !redirectPath.isEmpty()) {
				redirectPath += uriWithQueryString;
				String redirectMsg = "This is standby RM. The redirect url is: " + htmlEscapedUriWithQueryString;
				PrintWriter out = response.getWriter();
				out.println(redirectMsg);
				response.setHeader("Location", redirectPath);
				response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
				return;
			} else {
				boolean doRetry = true;
				String retryIntervalStr = request.getParameter(YarnWebParams.NEXT_REFRESH_INTERVAL);
				int retryInterval = 0;
				if (retryIntervalStr != null) {
					try {
						retryInterval = Integer.parseInt(retryIntervalStr.trim());
					} catch (NumberFormatException ex) {
						doRetry = false;
					}
				}
				int next = calculateExponentialTime(retryInterval);

				String redirectUrl = appendOrReplaceParamter(path + uriWithQueryString,
						YarnWebParams.NEXT_REFRESH_INTERVAL + "=" + (retryInterval + 1));
				if (redirectUrl == null || next > MAX_SLEEP_TIME) {
					doRetry = false;
				}
				String redirectMsg = doRetry ? "Can not find any active RM. Will retry in next " + next + " seconds."
						: "There is no active RM right now.";
				redirectMsg += "\nHA Zookeeper Connection State: " + rmWebApp.getHAZookeeperConnectionState();
				PrintWriter out = response.getWriter();
				out.println(redirectMsg);
				if (doRetry) {
					response.setHeader("Refresh", next + ";url=" + redirectUrl);
					response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
				}
			}
			return;
		}

		super.doFilter(request, response, chain);
	}

	private boolean shouldRedirect(RMWebApp rmWebApp, String uri) {
		return !uri.equals("/" + rmWebApp.wsName() + "/v1/cluster/info")
				&& !uri.equals("/" + rmWebApp.name() + "/cluster") && !NON_REDIRECTED_URIS.contains(uri);
	}

	private String appendOrReplaceParamter(String uri, String newQuery) {
		if (uri.contains(YarnWebParams.NEXT_REFRESH_INTERVAL + "=")) {
			return uri.replaceAll(YarnWebParams.NEXT_REFRESH_INTERVAL + "=[^&]+", newQuery);
		}
		try {
			URI oldUri = new URI(uri);
			String appendQuery = oldUri.getQuery();
			if (appendQuery == null) {
				appendQuery = newQuery;
			} else {
				appendQuery += "&" + newQuery;
			}

			URI newUri = new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(), appendQuery,
					oldUri.getFragment());

			return newUri.toString();
		} catch (URISyntaxException e) {
			return null;
		}
	}

	private static int calculateExponentialTime(int retries) {
		long baseTime = BASIC_SLEEP_TIME * (1L << retries);
		return (int) (baseTime * (randnum.nextDouble() + 0.5));
	}
}
