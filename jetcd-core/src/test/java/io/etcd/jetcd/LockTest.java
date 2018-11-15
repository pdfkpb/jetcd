/**
 * Copyright 2017 The jetcd authors
 *
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
package io.etcd.jetcd;

import com.google.common.base.Charsets;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.rules.ExpectedException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdClusterFactory;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Lock service test cases.
 */
public class LockTest {
  private static final EtcdCluster CLUSTER = EtcdClusterFactory.buildCluster("etcd-lock", 3 ,false);

  private static Lock lockClient;
  private static Lease leaseClient;
  private Set<ByteSequence> locksToRelease;

  private static final ByteSequence SAMPLE_NAME = ByteSequence.from("sample_name", Charsets.UTF_8);

  @BeforeClass
  public static void setUp() throws Exception {
    CLUSTER.start();

    Client client = Client.builder().endpoints(CLUSTER.getClientEndpoints()).build();

    lockClient = client.getLockClient();
    leaseClient = client.getLeaseClient();
  }

  @Before
  public void setUpEach() throws Exception {
    locksToRelease = new HashSet<>();
  }

  @After
  public void tearDownEach() throws Exception {
    for (ByteSequence lockKey : locksToRelease) {
      lockClient.unlock(lockKey).get();
    }
  }

  @Test
  public void testLockWithoutLease() throws Exception {
    CompletableFuture<LockResponse> feature = lockClient.lock(SAMPLE_NAME, 0);
    LockResponse response = feature.get();
    locksToRelease.add(response.getKey());

    assertNotNull(response.getHeader());
    assertNotNull(response.getKey());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  @Test
  public void testLockWithNotExistingLease() throws Exception {
    CompletableFuture<LockResponse> feature = lockClient.lock(SAMPLE_NAME, 123456);
    
    exception.expect(ExecutionException.class);
    LockResponse response = feature.get();
    locksToRelease.add(response.getKey());
  }

  @Test
  public void testLockWithLease() throws Exception {
    long lease = grantLease(5);
    CompletableFuture<LockResponse> feature = lockClient.lock(SAMPLE_NAME, lease);
    LockResponse response = feature.get();

    long startMillis = System.currentTimeMillis();

    CompletableFuture<LockResponse> feature2 = lockClient.lock(SAMPLE_NAME, 0);
    LockResponse response2 = feature2.get();

    long time = System.currentTimeMillis() - startMillis;

    assertNotEquals(response.getKey(), response2.getKey());
    assertTrue(String.format("Lease not runned out after 5000ms, was %dms", time),
        time >= 4500 && time <= 6000);

    locksToRelease.add(response.getKey());
    locksToRelease.add(response2.getKey());
  }

  @Test
  public void testLockAndUnlock() throws Exception {
    long lease = grantLease(20);
    CompletableFuture<LockResponse> feature = lockClient.lock(SAMPLE_NAME, lease);
    LockResponse response = feature.get();

    lockClient.unlock(response.getKey()).get();

    long startTime = System.currentTimeMillis();

    CompletableFuture<LockResponse> feature2 = lockClient.lock(SAMPLE_NAME, 0);
    LockResponse response2 = feature2.get();

    long time = System.currentTimeMillis() - startTime;

    locksToRelease.add(response2.getKey());

    assertNotEquals(response.getKey(), response2.getKey());
    assertTrue(String.format("Lease not unlocked, wait time was too long (%dms)", time),
        time <= 500);
  }

  private static long grantLease(long ttl) throws Exception {
    CompletableFuture<LeaseGrantResponse> feature = leaseClient.grant(ttl);
    LeaseGrantResponse response = feature.get();
    return response.getID();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    CLUSTER.close();
  }
}