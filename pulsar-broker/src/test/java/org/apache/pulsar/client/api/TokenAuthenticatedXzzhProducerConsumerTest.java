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
      "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiItMiIsImF1ZCI6InB1bHNhciIsImF1ZElkIjoiMCIsInJvbGVzIjoiYnJva2VyX2FkbWluLGZ1bmN0aW9uX2FkbWluLHByb3h5X2FkbWluLGFwcF9hZG1pbiIsImlzcyI6Inh6emguY29tIiwic3ViVHlwZSI6IjUiLCJleHAiOiIyMDMxLTA4LTE5IDIwOjIxOjEzIiwiaWF0IjoiMjAyMS0wOC0yNCAwOTozMzozMiIsImp0aSI6IjE3MjU5NTc0MTQwNCJ9.Vct9ccrSkhggvxdxXTJsq-w0r5-u3S2DGXhoeuktUx3wKpoVX7v3zE4kjgatDgzALkL7LRVuE9wSKpnEWgvrU-xNNcflyz1c8iqTppoBdgHOG_Cv119LVtqTq42YI5YZ3o3SkBGxOkj35e4aj9PszNElle1OsnLI2hFefrqjarUSkUyxoEWwv926IRwQj43BYczz1J__2DT8dT1Q9p9jmDjUQcXppE3X-DXN7v8wet5k51jfq9pRn94kLBWZ0993dXaapfsA_ZwNB1d2o73TSb4aHJgj1PDDfe1zV10nF9Mm3JXhf9sdAh50IxWd3ASJdBEMzMudY5stdYNOV35v_sofDX_7lmWChewQx1VixRZDeHpFtX3_2cvBtvNQTchHJ3urH6Jhzz-gNUavyEuLFhG4mkGYDiJBb4yPiOpk7frTzroryWKETmzw9DuRPOHK9duBLRFics2sv6A4MBfxlLVRJ5-6gLRbXx75-Bzjc0SkhZ0OQGcDl3CE9HcA4F2D2hwPtZDRseYSMCTGh3kx1SWDL0jW7y2RsHzPt_PWfKd4gon76VtO3ZuIj816aH_Q3ajNmtWxRIOZXn8RblRxbWTu1hp7duLW3Yavkzssv6Gbm31-2kRV7tIgVTerMN9rMi7xOEmpYaJfVJ_yEk3QrpxBnehnuoNbxDQAH9awX6c";

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
        "pulsarXzzhAuthenticationGrpcUrl",
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
