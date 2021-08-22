package org.apache.pulsar.client.impl.auth;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.AuthenticationDataProvider;

/**
 * @author liShan
 * @date 2021/8/21 10:18
 */
public class AuthenticationDataXzzh implements AuthenticationDataProvider {

  public static final String HTTP_HEADER_NAME = "Authorization";
  private static final long serialVersionUID = -5211475808553132454L;

  private final Supplier<String> tokenSupplier;

  public AuthenticationDataXzzh(Supplier<String> tokenSupplier) {
    this.tokenSupplier = tokenSupplier;
  }

  @Override
  public boolean hasDataForHttp() {
    return true;
  }

  @Override
  public Set<Entry<String, String>> getHttpHeaders() {
    return Collections.singletonMap(HTTP_HEADER_NAME, "Bearer " + getToken()).entrySet();
  }

  @Override
  public boolean hasDataFromCommand() {
    return true;
  }

  @Override
  public String getCommandData() {
    return getToken();
  }

  private String getToken() {
    try {
      return tokenSupplier.get();
    } catch (Throwable t) {
      throw new RuntimeException("failed to get client token", t);
    }
  }
}
