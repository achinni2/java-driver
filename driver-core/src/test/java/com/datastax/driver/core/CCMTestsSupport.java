/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.CreateCCM.TestMode;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestResult;
import org.testng.annotations.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_CLASS;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.*;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("unused")
public class CCMTestsSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCMTestsSupport.class);

    private static final AtomicInteger CCM_COUNTER = new AtomicInteger(1);

    private static final List<String> TEST_GROUPS = Lists.newArrayList("isolated", "short", "long", "stress", "duration");

    // A mapping of cassandra.yaml config options to their version requirements.
    // If a config is passed containing one of these options and the version requirement cannot be met
    // the option is simply filtered.
    private static final Map<String, VersionNumber> configVersionRequirements = ImmutableMap.<String, VersionNumber>builder()
            .put("enable_user_defined_functions", VersionNumber.parse("2.2.0"))
            .build();


    private static class ReadOnlyCCMBridge implements CCMAccess {

        private final CCMAccess delegate;

        private ReadOnlyCCMBridge(CCMAccess delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getClusterName() {
            return delegate.getClusterName();
        }

        @Override
        public InetSocketAddress addressOfNode(int n) {
            return delegate.addressOfNode(n);
        }

        @Override
        public VersionNumber getVersion() {
            return delegate.getVersion();
        }

        @Override
        public File getCcmDir() {
            return delegate.getCcmDir();
        }

        @Override
        public File getClusterDir() {
            return delegate.getClusterDir();
        }

        @Override
        public File getNodeDir(int n) {
            return delegate.getNodeDir(n);
        }

        @Override
        public File getNodeConfDir(int n) {
            return delegate.getNodeConfDir(n);
        }

        @Override
        public int getStoragePort() {
            return delegate.getStoragePort();
        }

        @Override
        public int getThriftPort() {
            return delegate.getThriftPort();
        }

        @Override
        public int getBinaryPort() {
            return delegate.getBinaryPort();
        }

        @Override
        public void setKeepLogs(boolean keepLogs) {
            delegate.setKeepLogs(keepLogs);
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void stop() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void forceStop() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void start(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void stop(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void forceStop(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void remove(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void add(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void add(int dc, int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void decommision(int n) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateConfig(Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateDSEConfig(Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateNodeConfig(int n, String key, Object value) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void updateNodeConfig(int n, Map<String, Object> configs) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void setWorkload(int node, String workload) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void waitForUp(int node) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }

        @Override
        public void waitForDown(int node) {
            throw new UnsupportedOperationException("This CCM cluster is read-only");
        }
    }

    private static class CCMTestConfig {

        private final List<CCMConfig> annotations;

        private CCMBridge.Builder ccmBuilder;

        public CCMTestConfig(List<CCMConfig> annotations) {
            this.annotations = annotations;
        }

        private int[] numberOfNodes() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.numberOfNodes().length > 0)
                    return ann.numberOfNodes();
            }
            return new int[]{1};
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private String version() {
            for (CCMConfig ann : annotations) {
                if (ann != null && !ann.version().isEmpty())
                    return ann.version();
            }
            return null;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean dse() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.dse().length > 0)
                    return ann.dse()[0];
            }
            return false;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean ssl() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.ssl().length > 0)
                    return ann.ssl()[0];
            }
            return false;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean auth() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.auth().length > 0)
                    return ann.auth()[0];
            }
            return false;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Map<String, Object> config() {
            Map<String, Object> config = new HashMap<String, Object>();
            config.put("start_rpc", false);
            for (int i = annotations.size() - 1; i == 0; i--) {
                CCMConfig ann = annotations.get(i);
                addConfigOptions(ann.config(), config);
            }
            return config;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Map<String, Object> dseConfig() {
            Map<String, Object> config = new HashMap<String, Object>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                addConfigOptions(ann.dseConfig(), config);
            }
            return config;
        }

        private void addConfigOptions(String[] conf, Map<String, Object> config) {
            String versionStr = version();
            VersionNumber version = VersionNumber.parse(versionStr != null ?
                    versionStr : CCMBridge.getCassandraVersion());
            for (String aConf : conf) {
                String[] tokens = aConf.split(":");
                if (tokens.length != 2)
                    fail("Wrong configuration option: " + aConf);
                String key = tokens[0];
                String value = tokens[1];
                // If we've detected a property with a version requirement, skip it if the version requirement
                // cannot be met.
                if (configVersionRequirements.containsKey(key)) {
                    VersionNumber requirement = configVersionRequirements.get(key);
                    if (version.compareTo(requirement) < 0) {
                        LOGGER.debug("Skipping inclusion of '{}' in cassandra.yaml since it requires >= C* {} and {} " +
                                "was detected.", aConf, requirement, version);
                        continue;
                    }
                }
                config.put(key, value);
            }
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Set<String> jvmArgs() {
            Set<String> args = new LinkedHashSet<String>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                Collections.addAll(args, ann.jvmArgs());
            }
            return args;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private Set<String> startOptions() {
            Set<String> args = new LinkedHashSet<String>();
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                Collections.addAll(args, ann.options());
            }
            return args;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createCcm() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createCcm().length > 0)
                    return ann.createCcm()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createCluster() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createCluster().length > 0)
                    return ann.createCluster()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createSession() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createSession().length > 0)
                    return ann.createSession()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean createKeyspace() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.createKeyspace().length > 0)
                    return ann.createKeyspace()[0];
            }
            return true;
        }

        @SuppressWarnings("SimplifiableIfStatement")
        private boolean dirtiesContext() {
            for (CCMConfig ann : annotations) {
                if (ann != null && ann.dirtiesContext().length > 0)
                    return ann.dirtiesContext()[0];
            }
            return false;
        }

        private CCMBridge.Builder ccmBuilder() {
            if (ccmBuilder == null) {
                ccmBuilder = CCMBridge.builder().withNodes(numberOfNodes());
                if (version() != null)
                    ccmBuilder.withVersion(version());
                if (dse())
                    ccmBuilder.withDSE();
                if (ssl())
                    ccmBuilder.withSSL();
                if (auth())
                    ccmBuilder.withAuth();
                for (Map.Entry<String, Object> entry : config().entrySet()) {
                    ccmBuilder.withCassandraConfiguration(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : dseConfig().entrySet()) {
                    ccmBuilder.withDSEConfiguration(entry.getKey(), entry.getValue());
                }
                for (String option : startOptions()) {
                    ccmBuilder.withCreateOptions(option);
                }
                for (String arg : jvmArgs()) {
                    ccmBuilder.withJvmArgs(arg);
                }
            }
            return ccmBuilder;
        }

        private Cluster.Builder clusterBuilder(Object testInstance) throws Exception {
            String methodName = null;
            Class<?> clazz = null;
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                if (!ann.clusterProvider().isEmpty()) {
                    methodName = ann.clusterProvider();
                }
                if (!ann.clusterProviderClass().equals(CCMConfig.Undefined.class)) {
                    clazz = ann.clusterProviderClass();
                }
            }
            if (methodName == null)
                methodName = "createClusterBuilder";
            if (clazz == null)
                clazz = testInstance.getClass();
            Method method = locateMethod(methodName, clazz);
            assert Cluster.Builder.class.isAssignableFrom(method.getReturnType());
            if (Modifier.isStatic(method.getModifiers())) {
                return (Cluster.Builder) method.invoke(null);
            } else {
                Object receiver = testInstance.getClass().equals(clazz) ? testInstance : instantiate(clazz);
                return (Cluster.Builder) method.invoke(receiver);
            }
        }

        @SuppressWarnings("unchecked")
        private Collection<String> fixtures(Object testInstance) throws Exception {
            String methodName = null;
            Class<?> clazz = null;
            for (int i = annotations.size() - 1; i >= 0; i--) {
                CCMConfig ann = annotations.get(i);
                if (!ann.fixturesProvider().isEmpty()) {
                    methodName = ann.fixturesProvider();
                }
                if (!ann.fixturesProviderClass().equals(CCMConfig.Undefined.class)) {
                    clazz = ann.fixturesProviderClass();
                }
            }
            if (methodName == null)
                methodName = "createTestFixtures";
            if (clazz == null)
                clazz = testInstance.getClass();
            Method method = locateMethod(methodName, clazz);
            assert Collection.class.isAssignableFrom(method.getReturnType());
            assert method.getGenericReturnType() instanceof ParameterizedType;
            assert String.class.equals(((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0]);
            if (Modifier.isStatic(method.getModifiers())) {
                return (Collection<String>) method.invoke(null);
            } else {
                Object receiver = testInstance.getClass().equals(clazz) ? testInstance : instantiate(clazz);
                return (Collection<String>) method.invoke(receiver);
            }
        }

        private int weight() {
            int weight = 0;
            weight += totalNodes() - 1;
            weight += config().size() - 1;
            weight += dseConfig().size();
            weight += dse() ? 1 : 0;
            weight += jvmArgs().size();
            weight += startOptions().size();
            weight += ssl() ? 1 : 0;
            weight += auth() ? 1 : 0;
            weight += version() != null ? 1 : 0;
            return weight;
        }

        private int totalNodes() {
            int nodes = 0;
            for (int nodesPerDc : numberOfNodes()) {
                nodes += nodesPerDc;
            }
            return nodes;
        }
    }

    private static class CCMTestContextKey {

        private final CCMBridge.Builder builder;

        private final int weight;

        private CCMTestContextKey(CCMBridge.Builder builder, int weight) {
            this.builder = builder;
            this.weight = weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CCMTestContextKey that = (CCMTestContextKey) o;
            return builder.equals(that.builder);
        }

        @Override
        public int hashCode() {
            return builder.hashCode();
        }
    }

    private static class CCMTestContext implements Runnable {

        private CCMAccess ccm;

        private final AtomicInteger refCount = new AtomicInteger(1);

        private volatile boolean evicted = false;

        private CCMTestContext(CCMAccess ccm) {
            this(ccm, false);
        }

        public CCMTestContext(CCMAccess ccm, boolean evicted) {
            this.ccm = ccm;
            this.evicted = evicted;
        }

        @Override
        public void run() {
            if (ccm != null)
                ccm.close();
            ccm = null;
        }

        private void markEvicted() {
            evicted = true;
        }

        private void maybeClose() {
            if (refCount.get() <= 0 && evicted)
                POOL.execute(this);
        }
    }

    private static class CCMTestContextLoader extends CacheLoader<CCMTestContextKey, CCMTestContext> {

        @Override
        public CCMTestContext load(CCMTestContextKey key) {
            return new CCMTestContext(key.builder.build());
        }

    }

    private static class CCMTestContextWeigher implements Weigher<CCMTestContextKey, CCMTestContext> {

        @Override
        public int weigh(CCMTestContextKey key, CCMTestContext value) {
            return key.weight;
        }

    }

    private static class CCMTestContextRemovalListener implements RemovalListener<CCMTestContextKey, CCMTestContext> {

        @Override
        public void onRemoval(RemovalNotification<CCMTestContextKey, CCMTestContext> notification) {
            CCMTestContext context = notification.getValue();
            if (context != null && context.ccm != null) {
                LOGGER.debug("Evicting: {} because of {}", context.ccm, notification.getCause());
                context.markEvicted();
                context.maybeClose();
            }
        }

    }

    /**
     * A LoadingCache that stores running CCM clusters.
     */
    private static final LoadingCache<CCMTestContextKey, CCMTestContext> CACHE;

    private static final ExecutorService POOL = MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2,
                    new ThreadFactoryBuilder().setNameFormat("ccm-removal-%d").build()));

    static {
        long maximumWeight;
        String numberOfNodes = System.getProperty("ccm.maxNumberOfNodes");
        if (numberOfNodes == null) {
            long freeMemoryMB = TestUtils.getFreeMemoryMB();
            if (freeMemoryMB < 1000)
                LOGGER.warn("Available memory below 1GB: {}MB, CCM clusters might fail to start", freeMemoryMB);
            // CCM nodes are started with -Xms500M -Xmx500M
            // so 1 "slot" is roughly 500MB
            // leave at least 2 slots out, with a minimum of 1 slot and a maximum of 6 slots
            long slotsAvailable = (freeMemoryMB / 500) - 2;
            maximumWeight = Math.min(6, Math.max(1, slotsAvailable));
        } else {
            maximumWeight = Integer.parseInt(numberOfNodes);
        }
        LOGGER.info("Maximum number of running CCM nodes: {}", maximumWeight);
        CACHE = CacheBuilder.newBuilder()
                .initialCapacity(3)
                .maximumWeight(maximumWeight)
                .weigher(new CCMTestContextWeigher())
                .removalListener(new CCMTestContextRemovalListener())
                .build(new CCMTestContextLoader());
    }

    private TestMode testMode;

    private CCMTestConfig ccmTestConfig;

    private CCMTestContext ccmTestContext;

    protected CCMAccess ccm;

    protected Cluster cluster;

    protected Session session;

    protected String keyspace;

    private boolean erroredOut = false;

    private Closer closer;

    /**
     * Hook invoked at the beginning of a test class to initialize CCM test context.
     *
     * @throws Exception
     */
    @BeforeClass(groups = {"isolated", "short", "long", "stress", "duration"})
    public void beforeTestClass() throws Exception {
        beforeTestClass(this);
    }

    /**
     * Hook invoked at the beginning of a test class to initialize CCM test context.
     * <p/>
     * Useful when this class is not a superclass of the test being run.
     *
     * @throws Exception
     */
    public void beforeTestClass(Object testInstance) throws Exception {
        testMode = determineTestMode(testInstance.getClass());
        if (testMode == PER_CLASS) {
            closer = Closer.create();
            initTestContext(testInstance, null);
            initTestCluster(testInstance);
            initTestSession();
            initTestKeyspace();
            initTestFixtures(testInstance);
        }
    }

    /**
     * Hook executed before each test method.
     *
     * @throws Exception
     */
    @BeforeMethod(groups = {"isolated", "short", "long", "stress", "duration"})
    public void beforeTestMethod(Method testMethod) throws Exception {
        beforeTestMethod(this, testMethod);
    }

    /**
     * Hook executed before each test method.
     * <p/>
     * Useful when this class is not a superclass of the test being run.
     *
     * @throws Exception
     */
    public void beforeTestMethod(Object testInstance, Method testMethod) throws Exception {
        if (isCcmEnabled(testMethod)) {
            if (closer == null)
                closer = Closer.create();
            if (testMode == PER_METHOD || erroredOut) {
                initTestContext(testInstance, testMethod);
                initTestCluster(testInstance);
                initTestSession();
                initTestKeyspace();
                initTestFixtures(testInstance);
            }
            assert ccmTestConfig != null;
            assert !ccmTestConfig.createCcm() || ccm != null;
        }
    }

    /**
     * Hook executed after each test method.
     */
    @AfterMethod(groups = {"isolated", "short", "long", "stress", "duration"}, alwaysRun = true)
    public void afterTestMethod(ITestResult tr) {
        if (isCcmEnabled(tr.getMethod().getConstructorOrMethod().getMethod())) {
            if (tr.getStatus() == ITestResult.FAILURE) {
                errorOut();
            }
            if (erroredOut || testMode == PER_METHOD) {
                closeCloseables();
                closeTestCluster();
            }
            if (testMode == PER_METHOD)
                closeTestContext();
        }
    }

    /**
     * Hook executed after each test class.
     */
    @AfterClass(groups = {"isolated", "short", "long", "stress", "duration"}, alwaysRun = true)
    public void afterTestClass() {
        if (testMode == PER_CLASS) {
            closeCloseables();
            closeTestCluster();
            closeTestContext();
        }
    }

    /**
     * Returns the cluster builder to use for this test.
     * <p/>
     * The default implementation returns a vanilla builder.
     * <p/>
     * It's not required to call {@link com.datastax.driver.core.Cluster.Builder#addContactPointsWithPorts},
     * it will be done automatically.
     *
     * @return The cluster builder to use for the tests.
     */
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder()
                // use a different codec registry for each cluster instance
                .withCodecRegistry(new CodecRegistry());
    }

    /**
     * Returns an alternate cluster builder to use for this test,
     * with no event debouncing.
     * <p/>
     * It's not required to call {@link com.datastax.driver.core.Cluster.Builder#addContactPointsWithPorts},
     * it will be done automatically.
     *
     * @return The cluster builder to use for the tests.
     */
    public Cluster.Builder createClusterBuilderNoDebouncing() {
        return Cluster.builder().withQueryOptions(TestUtils.nonDebouncingQueryOptions());
    }

    /**
     * The CQL statements to execute before the tests in this class.
     * Useful to create tables or other objects inside the test keyspace,
     * or to insert test data.
     * <p/>
     * Statements do not need to be qualified with keyspace name.
     * <p/>
     * The default implementation returns an empty list (no fixtures required).
     *
     * @return The DDL statements to execute before the tests.
     */
    public Collection<String> createTestFixtures() {
        return Collections.emptyList();
    }

    /**
     * Signals that the test has encountered an unexpected error.
     * <p/>
     * This method is automatically called when a test finishes with an unexpected exception
     * being thrown, but it is also possible to manually invoke it.
     * <p/>
     * Calling this method will close the current cluster and session.
     * The CCM data directory will be kept after the test session is finished,
     * for debugging purposes
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     */
    public void errorOut() {
        erroredOut = true;
        if (ccm != null)
            ccm.setKeepLogs(true);
    }

    /**
     * Returns the contact points to use to contact the CCM cluster.
     * <p/>
     * This method returns as many contact points as the number of nodes initially present in the CCM cluster,
     * according to {@link CCMConfig} annotations.
     * <p/>
     * On a multi-DC setup, this will include nodes in all data centers.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @return the contact points to use to contact the CCM cluster.
     */
    public List<InetSocketAddress> getInitialContactPoints() {
        assert ccmTestConfig != null;
        List<InetSocketAddress> contactPoints = new ArrayList<InetSocketAddress>();
        int n = 1;
        int[] numberOfNodes = ccmTestConfig.numberOfNodes();
        for (int dc = 1; dc <= numberOfNodes.length; dc++) {
            int nodesInDc = numberOfNodes[dc - 1];
            for (int i = 0; i < nodesInDc; i++) {
                contactPoints.add(new InetSocketAddress(ipOfNode(n), ccm.getBinaryPort()));
                n++;
            }
        }
        return contactPoints;
    }

    /**
     * Registers the given {@link Closeable} to be closed at the end of the current test method.
     * <p/>
     * This method should not be called before the test has started, nor after the test is finished.
     *
     * @param closeable The closeable to close
     * @return The closeable to close
     */
    public <T extends Closeable> T register(T closeable) {
        closer.register(closeable);
        return closeable;
    }

    private void initTestContext(Object testInstance, Method testMethod) throws Exception {
        erroredOut = false;
        ccmTestConfig = createCCMTestConfig(testInstance, testMethod);
        assert ccmTestConfig != null;
        if (ccmTestConfig.createCcm()) {
            if (ccmTestConfig.dirtiesContext()) {
                ccm = ccmTestConfig.ccmBuilder().build();
                ccmTestContext = new CCMTestContext(ccm, true);
                LOGGER.debug("Using dedicated {}", ccm);
            } else {
                CCMBridge.Builder builder = ccmTestConfig.ccmBuilder();
                int weight = ccmTestConfig.weight();
                CCMTestContextKey key = new CCMTestContextKey(builder, weight);
                CCMAccess ccm;
                ccmTestContext = CACHE.getIfPresent(key);
                if (ccmTestContext != null) {
                    ccmTestContext.refCount.incrementAndGet();
                    ccm = ccmTestContext.ccm;
                    LOGGER.debug("Reusing {}", ccm);
                } else {
                    try {
                        ccmTestContext = CACHE.get(key);
                        ccm = ccmTestContext.ccm;
                        LOGGER.debug("Using newly-created {}", ccm);
                    } catch (ExecutionException e) {
                        throw Throwables.propagate(e);
                    }
                }
                this.ccm = new ReadOnlyCCMBridge(ccm);
            }
            assert ccmTestContext != null;
            assert ccm != null;

        }
    }

    private void initTestCluster(Object testInstance) throws Exception {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster()) {
            Cluster.Builder builder = ccmTestConfig.clusterBuilder(testInstance);
            // add contact points only if the provided builder didn't do so
            if (builder.getContactPoints().isEmpty())
                builder.addContactPointsWithPorts(getInitialContactPoints());
            builder.withPort(ccm.getBinaryPort());
            cluster = register(builder.build());
            cluster.init();
        }
    }

    private void initTestSession() throws Exception {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster() && ccmTestConfig.createSession())
            session = register(cluster.connect());
    }

    private void initTestKeyspace() {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster() && ccmTestConfig.createSession() && ccmTestConfig.createKeyspace()) {
            try {
                keyspace = TestUtils.getAvailableKeyspaceName();
                LOGGER.debug("Using keyspace " + keyspace);
                session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
                session.execute("USE " + keyspace);
            } catch (Exception e) {
                errorOut();
                LOGGER.error("Could not create test keyspace", e);
                Throwables.propagate(e);
            }
        }
    }

    private void initTestFixtures(Object testInstance) throws Exception {
        if (ccmTestConfig.createCcm() && ccmTestConfig.createCluster() && ccmTestConfig.createSession()) {
            for (String stmt : ccmTestConfig.fixtures(testInstance)) {
                try {
                    session.execute(stmt);
                } catch (Exception e) {
                    errorOut();
                    LOGGER.error("Could not execute statement: " + stmt, e);
                    Throwables.propagate(e);
                }
            }
        }
    }

    private void closeTestContext() {
        if (ccmTestContext != null) {
            ccmTestContext.refCount.decrementAndGet();
            ccmTestContext.maybeClose();
        }
        ccmTestConfig = null;
        ccmTestContext = null;
        ccm = null;
    }

    private void closeTestCluster() {
        if (cluster != null && !cluster.isClosed())
            executeNoFail(new Runnable() {
                @Override
                public void run() {
                    cluster.close();
                }
            }, false);
        cluster = null;
        session = null;
        keyspace = null;
    }

    private void closeCloseables() {
        if (closer != null)
            executeNoFail(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    closer.close();
                    return null;
                }
            }, false);
    }

    private static boolean isCcmEnabled(Method testMethod) {
        Test ann = locateAnnotation(testMethod, Test.class);
        return !Collections.disjoint(Arrays.asList(ann.groups()), TEST_GROUPS);
    }

    private static CCMTestConfig createCCMTestConfig(Object testInstance, Method testMethod) throws Exception {
        ArrayList<CCMConfig> annotations = new ArrayList<CCMConfig>();
        CCMConfig ann = locateAnnotation(testMethod, CCMConfig.class);
        if (ann != null)
            annotations.add(ann);
        locateClassAnnotations(testInstance.getClass(), CCMConfig.class, annotations);
        return new CCMTestConfig(annotations);
    }

    private static TestMode determineTestMode(Class<?> testClass) {
        List<CreateCCM> annotations = locateClassAnnotations(testClass, CreateCCM.class, new ArrayList<CreateCCM>());
        if (!annotations.isEmpty())
            return annotations.get(0).value();
        return PER_CLASS;
    }

    private static <A extends Annotation> A locateAnnotation(Method testMethod, Class<? extends A> clazz) {
        if (testMethod == null)
            return null;
        testMethod.setAccessible(true);
        return testMethod.getAnnotation(clazz);
    }

    private static <A extends Annotation> List<A> locateClassAnnotations(Class<?> clazz, Class<? extends A> annotationClass, List<A> annotations) {
        A ann = clazz.getAnnotation(annotationClass);
        if (ann != null)
            annotations.add(ann);
        clazz = clazz.getSuperclass();
        if (clazz == null)
            return annotations;
        return locateClassAnnotations(clazz, annotationClass, annotations);
    }

    private static Method locateMethod(String methodName, Class<?> clazz) throws NoSuchMethodException {
        try {
            Method method = clazz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            clazz = clazz.getSuperclass();
            if (clazz == null)
                throw e;
            return locateMethod(methodName, clazz);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T instantiate(Class<? extends T> clazz) throws NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        if (clazz.getEnclosingClass() == null || Modifier.isStatic(clazz.getModifiers())) {
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (T) constructor.newInstance();
        } else {
            Class<?> enclosingClass = clazz.getEnclosingClass();
            Object enclosingInstance = enclosingClass.newInstance();
            Constructor<?> constructor = clazz.getDeclaredConstructor(enclosingClass);
            constructor.setAccessible(true);
            return (T) constructor.newInstance(enclosingInstance);
        }
    }

}