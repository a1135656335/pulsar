package org.apache.pulsar.broker.authentication;

import static org.junit.Assert.assertNotNull;

import java.util.Properties;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author liShan
 * @date 2021/08/20
 */
public class AuthenticationProviderXzzhTest {

  ServiceConfiguration config;
  Properties properties;
  AuthenticationProviderXzzh provider;

  @BeforeClass
  public void setup() throws Exception {

    // Set provider domain name
    properties = new Properties();
    properties.setProperty("pulsar.xzzh.authentication.grpc.url",
        "gateway-deploy-primary.smartcloud-service-istio.svc.cluster.local:10701");
    config = new ServiceConfiguration();
    config.setProperties(properties);

    // Initialize authentication provider
    provider = new AuthenticationProviderXzzh();
    provider.initialize(config);
  }

  @Test
  public void testAuthenticateSignedToken() throws Exception {

    AuthenticationDataSource authData = new AuthenticationDataCommand(
        "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiItMSIsImF1ZCI6Imxpc2hhbl9hZG1pbiIsImF1ZElkIjoiNDQ1MjQyMiIsImlzcyI6ImZlbG9yZC5jbiIsInN1YlR5cGUiOiIxIiwiZXhwIjoiMjAyMS0wOC0yMSAxNTowNDo0MSIsImlhdCI6IjIwMjEtMDgtMjAgMTU6MDQ6NDEiLCJqdGkiOiIxNzI1OTE1NDg5NTgifQ.j5-F-QwVocCh2W0bv4fya5QBdu77G1TPE764ked0Swf7HdcXNdaGzfzrviVIj8PxBFWviwsVWLBCE0muOGP7uuy_8b86LIIPmOMUYITVjRm0hv0zbvAu8ylHsSfhn8eH_QO0V6kdMOpNxtPUwJAFJe_LbZ08rd3fRuyVU8h7Mzg"
    );
    var authenticate = provider.authenticate(authData);
    assertNotNull(authenticate);
  }

}
