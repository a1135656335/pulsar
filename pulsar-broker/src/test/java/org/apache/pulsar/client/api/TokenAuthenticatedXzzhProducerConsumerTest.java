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
package org.apache.pulsar.client.api;

import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import java.net.URI;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Token authentication with: client: org.apache.pulsar.client.impl.auth.AuthenticationToken
 * broker: org.apache.pulsar.broker.authentication.AuthenticationProviderToken
 */
@Test(groups = "broker-api")
public class TokenAuthenticatedXzzhProducerConsumerTest extends ProducerConsumerBase {
  private static final Logger log =
      LoggerFactory.getLogger(TokenAuthenticatedXzzhProducerConsumerTest.class);

  // admin token created based on private_key.
  private final String ADMIN_TOKEN =
      "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiItMiIsImF1ZCI6InJjdSIsImF1ZElkIjoiMCIsInJvbGVzIjoiYnJva2VyX2FkbWluLGZ1bmN0aW9uX2FkbWluLGFwcF9hZG1pbiIsImlzcyI6Inh6emguY29tIiwic3ViVHlwZSI6IjUiLCJleHAiOiIyMDMxLTA4LTE5IDIwOjIxOjEzIiwiaWF0IjoiMjAyMS0wOC0yMyAwMDoyNTo1MCIsImp0aSI6IjE3MjU5NTc0MDY5MiJ9.hBrxsyMqXJExqWr-m0lvAc_Ux17vWCAVfVf7bfBeci2D06MqtEwM4b4WYKfF5AK_KFIhu-2b0RlhHYVtTEDd-3PyTZn5YEPq7uo8BE76zQEsfS8yvaZ-dcyU7GgEYb_rBl9AV3xc9Ztla_bAuCFla_6gj4RGebHLOuc05oG8F2yhWjVdfqgASlaaJgTKmdHBSE5PHUMR-NP8gWkZZT8BO3t4jKv5vA2N8y_YliwZVZW81PNvYkq_Nv_b9eN9cRtGl7Px6sX_g5P1VxsyhtdRRQ0ga6fBmBXPHSFE-AiCHJ0YB8knZie5igDzI7anV4yWQ2LOmnyIOZLSKAuCOE6btXZIfZiLkxE_CNi9U-qQFLIybZbGhs-jAvXdgOt0oPDD_1COdeWQ_tx7yfY7D6RyZDk-KhUYMkI32c4EmMk7Og05mPXOBei-KNRqyQnJaXy5JudA7IeKPfmXXEvLo_0sMNNdzuhNElHogpIPxNnh5xGWkC2L7XMJxL6OCPJEP3f9U4Zvw-Rz89CQ_tTYBTCdXXZs9QHTyOyy2e-fixqU6PxPACrFvaIBRxsvmwrC03m86cbr-jVL3uvzqM6uDUWR81sbgynnTAPLZkT74REGBNtRxJe9MJfQFPNmzzr8M-fatBc2D-VFtMn3oHgn_nyC8cmjQNDtR4zQm_G9Kn_Hj7Q";

  @BeforeMethod
  @Override
  protected void setup() throws Exception {
    conf.setAuthenticationEnabled(true);
    conf.setAuthorizationEnabled(true);

    Set<String> superUserRoles = new HashSet<>();
    superUserRoles.add("broker_admin");
    superUserRoles.add("function_admin");
    superUserRoles.add("app_admin");
    conf.setSuperUserRoles(superUserRoles);

    Set<String> providers = new HashSet<>();
    providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderXzzh");
    conf.setAuthenticationProviders(providers);
    conf.setAuthorizationProvider(
        "org.apache.pulsar.broker.authentication.PulsarAuthorizationProviderXzzh");

    conf.setClusterName("test");

    // Set provider domain name
    Properties properties = new Properties();
    properties.setProperty(
        "pulsar.xzzh.authentication.grpc.url",
        "gateway-deploy-primary.smartcloud-service-istio.svc.cluster.local:10701");
    conf.setProperties(properties);
    super.init();
  }

  // setup both admin and pulsar client
  // 测试自定义认证
  protected final void clientSetup() throws Exception {
    admin =
        spy(
            PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(
                    AuthenticationFactory.create(
                        "org.apache.pulsar.client.impl.auth.AuthenticationXzzh", ADMIN_TOKEN))
                .build());

    replacePulsarClient(
        PulsarClient.builder()
            .serviceUrl(new URI(pulsar.getBrokerServiceUrl()).toString())
            .statsInterval(0, TimeUnit.SECONDS)
            .authentication(
                AuthenticationFactory.create(
                    "org.apache.pulsar.client.impl.auth.AuthenticationXzzh", ADMIN_TOKEN)));
  }

  @AfterMethod(alwaysRun = true)
  @Override
  protected void cleanup() throws Exception {
    super.internalCleanup();
  }

  @DataProvider(name = "batch")
  public Object[][] codecProvider() {
    return new Object[][] {{0}, {1000}};
  }

  private void testSyncProducerAndConsumer() throws Exception {
    Consumer<byte[]> consumer =
        pulsarClient
            .newConsumer()
            .topic("persistent://my-property/my-ns/my-topic")
            .subscriptionName("my-subscriber-name")
            .subscribe();

    ProducerBuilder<byte[]> producerBuilder =
        pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic");

    Producer<byte[]> producer = producerBuilder.create();
    for (int i = 0; i < 10; i++) {
      String message = "my-message-" + i;
      producer.send(message.getBytes());
    }

    Message<byte[]> msg = null;
    Set<String> messageSet = Sets.newHashSet();
    for (int i = 0; i < 10; i++) {
      msg = consumer.receive(5, TimeUnit.SECONDS);
      String receivedMessage = new String(msg.getData());
      log.debug("Received message: [{}]", receivedMessage);
      String expectedMessage = "my-message-" + i;
      testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
    }
    // Acknowledge the consumption of all messages at once
    consumer.acknowledgeCumulative(msg);
    consumer.close();
  }

  @Test
  public void testTokenProducerAndConsumer() throws Exception {
    log.info("-- Starting {} test --", methodName);
    clientSetup();

    // test rest by admin
    admin
        .clusters()
        .createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
    admin
        .tenants()
        .createTenant(
            "my-property",
            new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
    admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

    // test protocol by producer/consumer
    testSyncProducerAndConsumer();

    log.info("-- Exiting {} test --", methodName);
  }
}
