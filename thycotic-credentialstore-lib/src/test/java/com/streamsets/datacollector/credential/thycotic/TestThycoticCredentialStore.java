/*
 * Copyright 2019 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.credential.thycotic;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import com.streamsets.datacollector.credential.thycotic.api.AuthRenewalTask;
import com.streamsets.datacollector.credential.thycotic.api.GetThycoticSecrets;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class TestThycoticCredentialStore {

  @Test
  public void testInitNullConfigs() {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    store = Mockito.spy(store);

    store.init(context);
    Mockito.when(context.getConfig(Mockito.any())).thenReturn(null);
    Assert.assertEquals(4, store.init(context).size());
  }

  @Test
  public void testNegativeValues() {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(ThycoticCredentialStore.CACHE_EXPIRATION_PROP)).thenReturn("-1");
    Mockito.when(context.getConfig(ThycoticCredentialStore.CREDENTIAL_REFRESH_PROP)).thenReturn("-2");
    Mockito.when(context.getConfig(ThycoticCredentialStore.CREDENTIAL_RETRY_PROP)).thenReturn("-3");

    store.init(context);
    Assert.assertEquals(0, store.getCacheExpirationSeconds());
    Assert.assertEquals(0, store.getCredentialRefreshSeconds());
    Assert.assertEquals(0, store.getCredentialRetrySeconds());

  }

  @Test
  public void testLifeCycle() {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(ThycoticCredentialStore.CACHE_EXPIRATION_PROP)).thenReturn("1");
    Mockito.when(context.getConfig(ThycoticCredentialStore.CREDENTIAL_REFRESH_PROP)).thenReturn("2");
    Mockito.when(context.getConfig(ThycoticCredentialStore.CREDENTIAL_RETRY_PROP)).thenReturn("3");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_URL)).thenReturn("h");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_USERNAME)).thenReturn("u");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_PASSWORD)).thenReturn("p");
    Mockito.doReturn(true).when(store).checkSecretServerConnection();

    store.init(context);
    Assert.assertEquals(1L, store.getCacheExpirationSeconds());
    Assert.assertEquals(2L, store.getCredentialRefreshSeconds());
    Assert.assertEquals(3L, store.getCredentialRetrySeconds());

    store.destroy();
  }

  @Test
  public void testStore() throws StageException {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);

    CloseableHttpClient closeableHttpClient = Mockito.mock(CloseableHttpClient.class);
    Mockito.when(store.getClient()).thenReturn(closeableHttpClient);

    AuthRenewalTask auth = Mockito.mock(AuthRenewalTask.class);
    Mockito.when(store.getAuth()).thenReturn(auth);
    Mockito.when(auth.getAccessToken()).thenReturn("t");

    GetThycoticSecrets secret = Mockito.mock(GetThycoticSecrets.class);
    Mockito.when(secret.getSecretField((Mockito.eq(closeableHttpClient)),
        Mockito.eq("t"),
        Mockito.eq("h"),
        Mockito.eq(1),
        Mockito.eq("n"),
        Mockito.eq("g")
    )).thenReturn("secret");
    Mockito.doReturn(secret).when(store).getSecret();

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_URL)).thenReturn("h");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_USERNAME)).thenReturn("a");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_PASSWORD)).thenReturn("s");
    Mockito.doReturn(true).when(store).checkSecretServerConnection();
    store.init(context);

    CredentialValue credential = store.get("g", "1-n", "");
    Assert.assertNotNull(credential);
    Assert.assertEquals("secret", credential.get());

    store.destroy();
  }

  @Test
  public void testCache() throws StageException, InterruptedException {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);

    CloseableHttpClient closeableHttpClient = Mockito.mock(CloseableHttpClient.class);
    Mockito.when(store.getClient()).thenReturn(closeableHttpClient);

    AuthRenewalTask auth = Mockito.mock(AuthRenewalTask.class);
    Mockito.when(store.getAuth()).thenReturn(auth);
    Mockito.when(auth.getAccessToken()).thenReturn("t");

    GetThycoticSecrets secret = Mockito.mock(GetThycoticSecrets.class);
    Mockito.when(secret.getSecretField((Mockito.eq(closeableHttpClient)),
        Mockito.eq("t"),
        Mockito.eq("h"),
        Mockito.eq(1),
        Mockito.eq("n"),
        Mockito.eq("g")
    )).thenReturn("secret");
    Mockito.doReturn(secret).when(store).getSecret();

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_URL)).thenReturn("h");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_USERNAME)).thenReturn("u");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_PASSWORD)).thenReturn("p");

    Mockito.when(context.getConfig(Mockito.eq(ThycoticCredentialStore.CACHE_EXPIRATION_PROP))).thenReturn("200");
    Mockito.doReturn(true).when(store).checkSecretServerConnection();

    Assert.assertTrue(store.init(context).isEmpty());
    CredentialValue credential1 = store.get("g", "1-n", "");
    Assert.assertNotNull(credential1);
    Assert.assertEquals("secret", credential1.get());

    CredentialValue credential2 = store.get("g", "1-n", "");
    Assert.assertSame(credential1, credential2);

    store.destroy();
  }

  @Test
  public void testThycoticCredentialValueOptions() throws StageException {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_URL)).thenReturn("h");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_USERNAME)).thenReturn("u");
    Mockito.when(context.getConfig(ThycoticCredentialStore.THYCOTIC_SECRET_SERVER_PASSWORD)).thenReturn("p");
    Mockito.doReturn(true).when(store).checkSecretServerConnection();

    Assert.assertTrue(store.init(context).isEmpty());

    CredentialValue c = store.get("g", "1-n", "refresh=1,retry=2");
    Assert.assertNotNull(c);
    ThycoticCredentialStore.ThycoticCredentialValue cc = (ThycoticCredentialStore.ThycoticCredentialValue) c;
    Assert.assertEquals(1L, cc.getRefreshSeconds());
    Assert.assertEquals(2L, cc.getRetrySeconds());

    store.destroy();
  }

  @Test
  public void testCacheEncodeDecode() {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    String encoded = store.encode("g", "n", "o");
    Assert.assertArrayEquals(new String[]{"g", "n", "o"}, store.decode(encoded));
    encoded = store.encode("g", "n", "");
    Assert.assertArrayEquals(new String[]{"g", "n", ""}, store.decode(encoded));
  }

  @Test
  public void testEncodeDecode() {
    ThycoticCredentialStore store = new ThycoticCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    Assert.assertEquals(
        "g" + ThycoticCredentialStore.DELIMITER_FOR_CACHE_KEY + "n" + ThycoticCredentialStore.DELIMITER_FOR_CACHE_KEY
            + "o",
        store.encode("g", "n", "o")
    );
    Assert.assertArrayEquals(new String[]{"g", "n", "o"},
        store.decode("g" + ThycoticCredentialStore.DELIMITER_FOR_CACHE_KEY + "n" + ThycoticCredentialStore
            .DELIMITER_FOR_CACHE_KEY + "o")
    );
  }
}
