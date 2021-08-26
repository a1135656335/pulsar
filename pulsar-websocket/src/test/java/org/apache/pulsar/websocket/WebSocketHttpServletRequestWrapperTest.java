/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.websocket;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.eclipse.jetty.websocket.servlet.UpgradeHttpServletRequest;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/** WebSocketHttpServletRequestWrapper test. */
public class WebSocketHttpServletRequestWrapperTest {
  private static final String TOKEN =
      "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiItMiIsImF1ZCI6InB1bHNhciIsImF1ZElkIjoiMCIsInJvbGVzIjoiYnJva2VyX2FkbWluLGZ1bmN0aW9uX2FkbWluLHByb3h5X2FkbWluLGFwcF9hZG1pbiIsImlzcyI6Inh6emguY29tIiwic3ViVHlwZSI6IjUiLCJleHAiOiIyMDMxLTA4LTE5IDIwOjIxOjEzIiwiaWF0IjoiMjAyMS0wOC0yNSAyMDo0NzozOSIsImp0aSI6IjI3MjgxNDQ0MDQ4MCJ9.a5NA_iDM4iBh02KaVzWVHRjzzMsBVIQVybvhykAUXZnu3EfT7lyHWAGRtUXH7EQHxE07QtGSrkwUecUDbMAc6rmhN_W0qG7neMuA0ZH7Wl3i6sx-Lz6IoyHxTwTu_J2DuF-GSqPVhoyjktHCGnlrAe6khvZHZO8MzTRU17gpsHXUHL0f8YcB54TLO3QKYtDjZDniAnikp3t9Q_XgSd584zWS3sU4SZfdJAL0T8rtyFvk2n3oc6XmtmDQ1ev7qh6XwbJMxAg3ckk5dQJg2SPlDyeqmqEgZLjUPIWMLd2stll7WPHPwAN_XGiG7AbycMtsJxxWFL_iT5MIoYC1lSzUUk6RxfSj6Zm-hfno043XzDZSkw5m5MV_oH8l9rSHQ5s_x4vdqBROEQSy65zZEJFuWYzK0B8Ur5tD12f8lAXBU8fk7GK3NInCU23O9YSCHLr9-86DPLSHc9bNtesw-F93BjnKwWEtZr-TxmiDWcoM-JPZF97yfpsim-Urp_Fe3lTNwBowOUmUmMNMrXaCcXZrRxurFOUau8nxXxpZcHzrQ6lEly4L4myEqJDocu8mX30h890NjumSkEHZSN2Ng5a7lc9p-OsO2Gi5V4nEv7bM2XLg8p7icXQyMDZjJ94S9joSzkyNlPg4-DzYxlEY_MWf2L-GuylNUpUrP49NPulOeGc";
  private static final String BEARER_TOKEN =
      WebSocketHttpServletRequestWrapper.HTTP_HEADER_VALUE_PREFIX + TOKEN;

  @Test
  public void testTokenParamWithBearerPrefix() {
    UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
    Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
        .thenReturn(BEARER_TOKEN);

    WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
        new WebSocketHttpServletRequestWrapper(httpServletRequest);
    Assert.assertEquals(
        BEARER_TOKEN,
        webSocketHttpServletRequestWrapper.getHeader(
            WebSocketHttpServletRequestWrapper.HTTP_HEADER_NAME));
  }

  @Test
  public void testTokenParamWithOutBearerPrefix() {
    UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
    Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
        .thenReturn(TOKEN);

    WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
        new WebSocketHttpServletRequestWrapper(httpServletRequest);
    Assert.assertEquals(
        BEARER_TOKEN,
        webSocketHttpServletRequestWrapper.getHeader(
            WebSocketHttpServletRequestWrapper.HTTP_HEADER_NAME));
  }

  @Test
  public void mockRequestTest() throws Exception {
    WebSocketProxyConfiguration config =
        PulsarConfigurationLoader.create(
            this.getClass().getClassLoader().getResource("websocket.conf").getFile(),
            WebSocketProxyConfiguration.class);
    //        String publicKeyPath =
    // this.getClass().getClassLoader().getResource("my-public.key").getFile();
    //        config.getProperties().setProperty("tokenPublicKey", publicKeyPath);
    Set<String> providers = new HashSet<>();
    providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderXzzh");
    config.setAuthenticationProviders(providers);
    config.setAuthorizationProvider(
        "org.apache.pulsar.broker.authentication.PulsarAuthorizationProviderXzzh");
    Properties properties = new Properties();
    properties.setProperty(
        "pulsar.xzzh.authentication.grpc.url",
        "gateway-deploy-primary.smartcloud-service-istio.svc.cluster.local:10701");
    config.setProperties(properties);
    WebSocketService service = new WebSocketService(config);
    service.start();

    UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
    Mockito.when(httpServletRequest.getRemoteAddr()).thenReturn("192.168.30.130");
    Mockito.when(httpServletRequest.getRemotePort()).thenReturn(8080);
    Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
        .thenReturn(TOKEN);
    WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
        new WebSocketHttpServletRequestWrapper(httpServletRequest);
    String ret =
        service
            .getAuthenticationService()
            .authenticateHttpRequest(webSocketHttpServletRequestWrapper);
    Assert.assertEquals("test-user", ret);
  }
}
