/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.SaslException;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KerberosSaslHelper {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosSaslHelper.class);

  public static TTransport createSubjectAssumedTransport(
      Subject subject,
      String serverPrincipal,
      TTransport underlyingTransport,
      Map<String, String> saslProps)
      throws SaslException {
    String[] names = KerberosUtils.splitPrincipal(serverPrincipal);
    TTransport saslTransport =
        new TSaslClientTransport(
            "GSSAPI", null, names[0], names[1], saslProps, null, underlyingTransport);
    return new TSubjectTransport(saslTransport, subject);
  }

  public static TTransport getTokenTransport(
      String tokenStr, String host, TTransport underlyingTransport, Map<String, String> saslProps)
      throws SaslException {
    try {
      return createClientTransport(
          null, host, "DIGEST-MD5", tokenStr, underlyingTransport, saslProps);
    } catch (IOException e) {
      throw new SaslException("Failed to open client transport", e);
    }
  }

  /**
   * Create a client-side SASL transport that wraps an underlying transport.
   *
   * @param mechanism The authentication method to use. DIGEST-MD5, GSSAPI.
   * @param serverPrincipal The Kerberos principal of the target server.
   * @param underlyingTransport The underlying transport mechanism, usually a TSocket.
   * @param saslProps the sasl properties to create the client with
   */
  public static TTransport createClientTransport(
      String serverPrincipal,
      String host,
      String mechanism,
      String tokenStrForm,
      final TTransport underlyingTransport,
      final Map<String, String> saslProps)
      throws IOException {

    Subject subject;
    TTransport saslTransport;
    switch (mechanism) {
      case "GSSAPI":
        String canonicalServerPrincipal = KerberosUtils.canonicalPrincipal(serverPrincipal, host);
        String[] names = KerberosUtils.splitPrincipal(canonicalServerPrincipal);
        try {
          subject = HadoopShim.UserGroupInformation.getCurrentSubject();
          return Subject.doAs(
              subject,
              (PrivilegedExceptionAction<TSubjectTransport>)
                  () -> {
                    TTransport saslTransport1 =
                        new TSaslClientTransport(
                            mechanism,
                            null,
                            names[0],
                            names[1],
                            saslProps,
                            null,
                            underlyingTransport);
                    Subject subject1 = HadoopShim.UserGroupInformation.getCurrentSubject();
                    return new TSubjectTransport(saslTransport1, subject1);
                  });
        } catch (Exception se) {
          throw new IOException("Could not instantiate SASL transport", se);
        }

      case "DIGEST-MD5":
        Object token = HadoopShim.Token.newInstance();
        HadoopShim.Token.decodeFromUrlString(token, tokenStrForm);
        byte[] identifier = HadoopShim.Token.getIdentifier(token);
        byte[] password = HadoopShim.Token.getPassword(token);
        saslTransport =
            new TSaslClientTransport(
                mechanism,
                null,
                null,
                "default",
                saslProps,
                new SaslClientCallbackHandler(identifier, password),
                underlyingTransport);
        subject = HadoopShim.UserGroupInformation.getCurrentSubject();
        return new TSubjectTransport(saslTransport, subject);

      default:
        throw new IOException("Unsupported authentication mechanism: " + mechanism);
    }
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    public SaslClientCallbackHandler(byte[] identifier, byte[] password) {
      this.userName = encodeIdentifier(identifier);
      this.userPassword = encodePassword(password);
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        LOG.debug("SASL client callback: setting username: {}", userName);
        nc.setName(userName);
      }
      if (pc != null) {
        LOG.debug("SASL client callback: setting userPassword");
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        LOG.debug("SASL client callback: setting realm: {}", rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }

    static String encodeIdentifier(byte[] identifier) {
      return Base64.getEncoder().encodeToString(identifier);
    }

    static char[] encodePassword(byte[] password) {
      return Base64.getEncoder().encodeToString(password).toCharArray();
    }
  }

  private KerberosSaslHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }
}
