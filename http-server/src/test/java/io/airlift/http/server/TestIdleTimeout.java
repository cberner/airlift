/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.http.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIdleTimeout
{
    private static final Logger log = Logger.get(TestIdleTimeout.class);

    @Test
    public void test()
            throws Exception
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node.environment", "test")
                .put("http-server.http.port", "6666")
                .build();

        ConfigurationFactory configFactory = new ConfigurationFactory(properties);
        Injector injector = Guice.createInjector(
                new NodeModule(),
                new HttpServerModule(),
                new ConfigurationModule(configFactory),
                new EventModule(),
                binder -> {
                    binder.bind(Servlet.class).annotatedWith(TheServlet.class).to(DummyServlet.class);
                });

        HttpServer server = injector.getInstance(HttpServer.class);
        server.start();
        HttpClient client = new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(100, TimeUnit.MILLISECONDS)));

        Request request = prepareGet()
                .setUri(URI.create("http://localhost:6666/"))
                .build();

        for (int i = 0; i < 100; i++) {
            // If I comment out this sleep() then the test passes
            Thread.sleep(new Random().nextInt(12) + 90);

            long start = System.nanoTime();
            HttpClient.HttpResponseFuture<StringResponseHandler.StringResponse> future = client.executeAsync(request, createStringResponseHandler());
            Futures.addCallback(future, new FutureCallback<StringResponseHandler.StringResponse>() {
                @Override
                public void onSuccess(@Nullable StringResponseHandler.StringResponse result)
                {
                    log.info("ok");
                }

                @Override
                public void onFailure(Throwable t)
                {
                    // If I put a breakpoint here, I can see an idle timeout exception on line 172 of AbstractConnection.java
                    // it would be helpful if that exception was passed to the client, instead of the one that's constructed
                    // on line 98 of HttpConnectionOverHTTP.java
                    log.warn("not ok");
                }
            });

            int statusCode;
            try {
                statusCode = future.get().getStatusCode();
            }
            catch (Exception e) {
                if (!(e.getCause().getCause() instanceof TimeoutException)) {
                    throw e;
                }
                // timeout is expected only if it has been more than 100ms since we sent the request
                Duration duration = Duration.nanosSince(start);
                log.warn(e, "Timeout after %s", duration);
                assertTrue(duration.toMillis() > 100);
                continue;
            }
            assertEquals(statusCode, Response.Status.OK.getStatusCode());
        }
    }

    public static class DummyServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException
        {
            resp.setStatus(HttpServletResponse.SC_OK);
            if (req.getUserPrincipal() != null) {
                resp.getOutputStream().write(req.getUserPrincipal().getName().getBytes());
            }
        }
    }
}
