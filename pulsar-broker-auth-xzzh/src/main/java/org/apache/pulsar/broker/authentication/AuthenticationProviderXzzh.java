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
package org.apache.pulsar.broker.authentication;

import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.ProtoMsg.Jwt;
import org.apache.pulsar.broker.authentication.ProtoMsg.ResultPulsar;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liShan
 * @date 2021/8/20 11:35
 */
public class AuthenticationProviderXzzh implements AuthenticationProvider {

  static final String HTTP_HEADER_VALUE_PREFIX = "Bearer:";
  static final String HTTP_HEADER_VALUE_PREFIX_V2 = "Bearer ";
  private static final Logger log = LoggerFactory.getLogger(AuthenticationProviderXzzh.class);
  private static final String AUTHENTICATION_GRPC_SERVER_URL = "pulsarXzzhAuthenticationGrpcUrl";
  private static final String AUTHENTICATION_GRPC_KEEPALIVE_DURATION =
      "pulsarXzzhAuthenticationGrpckeepAliveDuration";
  private org.apache.pulsar.broker.authentication.AuthServerGrpc.AuthServerBlockingStub
      authServerGrpc = null;

  public static String getToken(AuthenticationDataSource authData) throws AuthenticationException {
    if (authData.hasDataFromCommand()) {
      var token = HTTP_HEADER_VALUE_PREFIX_V2 + authData.getCommandData();
      return validateToken(token);
    } else if (authData.hasDataFromHttp()) {
      // Authentication HTTP request. The format here should be compliant to RFC-6750
      // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg: Authorization: Bearer xxxxxxxxxxxxx
      // 兼容平台，允许 Bearer:xxxxxxxxxxxxx
      String httpHeaderValue = authData.getHttpHeader(HttpHeaders.AUTHORIZATION);
      if (httpHeaderValue == null) {
        throw new AuthenticationException("Invalid HTTP Authorization header");
      }
      var prefix = httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX);
      var prefixV2 = httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX_V2);
      if (!prefix && !prefixV2) {
        throw new AuthenticationException("Invalid HTTP Authorization header");
      }
      // Remove prefix
      return validateToken(httpHeaderValue);
    } else {
      throw new AuthenticationException("No token credentials passed");
    }
  }

  private static String validateToken(final String token) throws AuthenticationException {
    if (StringUtils.isNotBlank(token)) {
      return token;
    }
    throw new AuthenticationException("Blank token found");
  }

  /**
   * 初始化一个http客户端
   *
   * @param config broker config object
   * @throws IOException IOException
   */
  @Override
  public void initialize(ServiceConfiguration config) throws IOException {
    int maxRetryAttempts = 3;
    long keepAliveDuration = 5;
    String url;
    if (config.getProperty(AUTHENTICATION_GRPC_SERVER_URL) != null) {
      url = (String) config.getProperty(AUTHENTICATION_GRPC_SERVER_URL);
    } else {
      throw new IOException("No authentication grpc server url");
    }
    if (config.getProperty(AUTHENTICATION_GRPC_KEEPALIVE_DURATION) != null) {
      keepAliveDuration = (Long) config.getProperty(AUTHENTICATION_GRPC_KEEPALIVE_DURATION);
    }
    var channel =
        ManagedChannelBuilder.forTarget(url)
            .usePlaintext()
            .keepAliveTime(keepAliveDuration, TimeUnit.MINUTES)
            .maxRetryAttempts(maxRetryAttempts)
            .build();
    this.authServerGrpc =
        org.apache.pulsar.broker.authentication.AuthServerGrpc.newBlockingStub(channel);
  }

  @Override
  public String getAuthMethodName() {
    return "xzzh";
  }

  @Override
  public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
    try {
      String token;
      token = getToken(authData);
      String role = getPrincipal(token);
      AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
      return role;
    } catch (AuthenticationException exception) {
      AuthenticationMetrics.authenticateFailure(
          getClass().getSimpleName(), getAuthMethodName(), exception.getMessage());
      throw exception;
    }
  }

  /**
   * 解析jwt,jwt 格式为：Bearer xxxxxxxxxxxxx
   *
   * @param jwt jwt
   * @return 返回一个角色
   */
  public String getPrincipal(String jwt) {
    int ok = 2000;
    ResultPulsar resultPulsar =
        authServerGrpc.decodeAndVerifyV2(Jwt.newBuilder().setJwt(jwt).build());
    if (ok == resultPulsar.getStatusCode()) {
      var info = resultPulsar.getPulsarUserInfo();
      return StringUtils.join(info.getRolesList(), ",");
    }
    return "";
  }

  @Override
  public void close() {
    log.info("close");
  }
}
