

package alluxio.yarn;


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public final class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);

  /**
   * Resources needed by the master and worker containers. Yarn will copy these to the container
   * before running the container's command.
   */
  private static final List<String> LOCAL_RESOURCE_NAMES =
      Lists.newArrayList(YarnUtils.ALLUXIO_TARBALL, YarnUtils.ALLUXIO_SETUP_SCRIPT);

  private final int mNumWorkers;
  private final String mMasterAddress;
  private final String mResourcePath;

  private final YarnConfiguration mYarnConf = new YarnConfiguration();
  /** The count starts at 1, then becomes 0 when the application is done. */
  private final CountDownLatch mApplicationDoneLatch;

  /** Client to talk to Resource Manager. */
  private final AMRMClientAsync<ContainerRequest> mRMClient;
  /** Client to talk to Node Manager. */
  private final NMClient mNMClient;
  /** Client Resource Manager Service. */
  private final YarnClient mYarnClient;
  /** Network address of the container allocated for Alluxio master. */
  private String mMasterContainerNetAddress;

  private final Configuration mAlluxioConf;

  private volatile ContainerAllocator mContainerAllocator;

  /**
   * A factory which creates an AMRMClientAsync with a heartbeat interval and callback handler.
   */
  public interface AMRMClientAsyncFactory {
    /**
     * @param heartbeatMs the interval at which to send heartbeats to the resource manager
     * @param handler a handler for callbacks from the resource manager
     * @return a client for making requests to the resource manager
     */
    AMRMClientAsync<ContainerRequest> createAMRMClientAsync(int heartbeatMs,
        CallbackHandler handler);
  }

  /** Security tokens for HDFS. */
  private ByteBuffer mAllTokens;

  /**
   * Convenience constructor which uses the default Alluxio configuration.
   *
   * @param numWorkers the number of workers to launch
   * @param masterAddress the address at which to start the Alluxio master
   * @param resourcePath an hdfs path shared by all yarn nodes which can be used to share resources
   * @param alluxioConf Alluxio configuration
   */
  public ApplicationMaster(int numWorkers, String masterAddress, String resourcePath,
      Configuration alluxioConf) {
    this(numWorkers, masterAddress, resourcePath, YarnClient.createYarnClient(),
        NMClient.createNMClient(), new AMRMClientAsyncFactory() {
          @Override
          public AMRMClientAsync<ContainerRequest> createAMRMClientAsync(int heartbeatMs,
              CallbackHandler handler) {
            return AMRMClientAsync.createAMRMClientAsync(heartbeatMs, handler);
          }
        }, alluxioConf);
  }

  /**
   * Constructs an {@link ApplicationMaster}.
   *
   * Clients will be initialized and started during the {@link #start()} method.
   *
   * @param numWorkers the number of workers to launch
   * @param masterAddress the address at which to start the Alluxio master
   * @param resourcePath an hdfs path shared by all yarn nodes which can be used to share resources
   * @param yarnClient the client to use for communicating with Yarn
   * @param nMClient the client to use for communicating with the node manager
   * @param amrmFactory a factory for creating an {@link AMRMClientAsync}
   * @param alluxioConf Alluxio configuration
   */
  public ApplicationMaster(int numWorkers, String masterAddress, String resourcePath,
      YarnClient yarnClient, NMClient nMClient, AMRMClientAsyncFactory amrmFactory,
      Configuration alluxioConf) {
    mNumWorkers = numWorkers;
    mMasterAddress = masterAddress;
    mResourcePath = resourcePath;
    mApplicationDoneLatch = new CountDownLatch(1);
    mYarnClient = yarnClient;
    mNMClient = nMClient;
    mAlluxioConf = alluxioConf;
    // Heartbeat to the resource manager every 500ms.
    mRMClient = amrmFactory.createAMRMClientAsync(500, this);
  }

  /**
   * @param args Command line arguments to launch application master
   */
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("num_workers", true, "Number of Alluxio workers to launch. Default 1");
    options.addOption("master_address", true, "(Required) Address to run Alluxio master");
    options.addOption("resource_path", true,
        "(Required) HDFS path containing the Application Master");

    try {
      LOG.info("Starting Application Master with args {}", Arrays.toString(args));
      final CommandLine cliParser = new GnuParser().parse(options, args);
      Configuration alluxioConf = new Configuration();
      YarnConfiguration conf = new YarnConfiguration();
      UserGroupInformation.setConfiguration(conf);
      if (UserGroupInformation.isSecurityEnabled()) {
        String user = System.getenv("ALLUXIO_USER");
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        for (Token token : UserGroupInformation.getCurrentUser().getTokens()) {
          ugi.addToken(token);
        }
        LOG.info("UserGroupInformation: " + ugi);
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            runApplicationMaster(cliParser, alluxioConf);
            return null;
          }
        });
      } else {
        runApplicationMaster(cliParser, alluxioConf);
      }
    } catch (Exception e) {
      LOG.error("Error running Application Master", e);
      System.exit(1);
    }
  }

  /**
   * Run the application master.
   *
   * @param cliParser client arguments parser
   */
  private static void runApplicationMaster(final CommandLine cliParser,
      Configuration alluxioConf) throws Exception {
    int numWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));
    String masterAddress = "172.16.12.26";// cliParser.getOptionValue("master_address");
    String resourcePath = "hdfs://172.16.12.26:8020/user/root";//cliParser.getOptionValue("resource_path");

    ApplicationMaster applicationMaster =
        new ApplicationMaster(numWorkers, masterAddress, resourcePath, alluxioConf);
    applicationMaster.start();
    applicationMaster.requestAndLaunchContainers();
    applicationMaster.waitForShutdown();
    applicationMaster.stop();
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    for (Container container : containers) {
      mContainerAllocator.allocateContainer(container);
    }
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    for (ContainerStatus status : statuses) {
      // Releasing worker containers because we already have workers on their host will generate a
      // callback to this method, so we use debug instead of error.
      if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
        LOG.debug("Aborted container {}", status.getContainerId());
      } else {
        LOG.info("Container {} completed with exit status {}", status.getContainerId(),
            status.getExitStatus());
      }
    }
    mApplicationDoneLatch.countDown();
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updated) {}

  @Override
  public void onShutdownRequest() {

  }

  @Override
  public void onError(Throwable t) {
    LOG.error("Error reported by resource manager", t);
  }

  @Override
  public float getProgress() {
    return 0;
  }

  /**
   * Starts the application master.
   */
  public void start() throws IOException, YarnException {
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials =
          UserGroupInformation.getCurrentUser().getCredentials();
      DataOutputBuffer credentialsBuffer = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(credentialsBuffer);
      // Now remove the AM -> RM token so that containers cannot access it.
      Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        Token<?> token = iter.next();
        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          iter.remove();
        }
      }
      mAllTokens = ByteBuffer.wrap(credentialsBuffer.getData(), 0, credentialsBuffer.getLength());
    }
    mNMClient.init(mYarnConf);
    mNMClient.start();

    mRMClient.init(mYarnConf);
    mRMClient.start();

    mYarnClient.init(mYarnConf);
    mYarnClient.start();

    // Register with ResourceManager
    String hostname = "nn3";
    mRMClient.registerApplicationMaster(hostname, 0 /* port */, "" /* tracking url */);
    LOG.info("ApplicationMaster registered");
  }

  public static synchronized String getLocalHostName(int timeoutMs) {


    return "";
  }

  /**
   * Submits requests for containers until the master and all workers are launched.
   */
  public void requestAndLaunchContainers() throws Exception {

    Resource workerResource = Records.newRecord(Resource.class);
    workerResource.setMemory(1024);
    workerResource.setVirtualCores(1);
    mContainerAllocator = new ContainerAllocator("task", 1, 10,
        workerResource, mYarnClient, mRMClient);
    List<Container> workerContainers = mContainerAllocator.allocateContainers();
    for (Container container : workerContainers) {
      launchWorkerContainer(container);
      break;
    }
    LOG.info("container are launched");
  }

  private boolean masterExists() {
    try {
      URL myURL = new URL("http://nn3:7000");
      LOG.debug("Checking for master at: " + myURL.toString());
      HttpURLConnection connection = (HttpURLConnection) myURL.openConnection();
      connection.setRequestMethod("GET");
      int resCode = connection.getResponseCode();
      LOG.debug("Response code from master was: " + Integer.toString(resCode));
      connection.disconnect();
      return resCode == HttpURLConnection.HTTP_OK;
    } catch (MalformedURLException e) {
      LOG.error("Malformed URL in attempt to check if master is running already", e);
    } catch (IOException e) {
      LOG.debug("No existing master found", e);
    }
    return false;
  }

  /**
   * @throws InterruptedException if interrupted while awaiting shutdown
   */
  public void waitForShutdown() throws InterruptedException {
    mApplicationDoneLatch.await();
  }

  /**
   * Shuts down the application master, unregistering it from Yarn and stopping its clients.
   */
  public void stop() {
    try {
      mRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    } catch (YarnException e) {
      LOG.error("Failed to unregister application", e);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }
    mRMClient.stop();
    // TODO(andrew): Think about whether we should stop mNMClient here
    mYarnClient.stop();
  }


  private void launchWorkerContainer(Container container) {
    String command = "pwd&&ls&&sleep 1m &&ls";

    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    ctx.setCommands(Lists.newArrayList(command));
    ctx.setLocalResources(setupLocalResources(mResourcePath));
    Map<String,String> env = new HashMap<>();
    ctx.setEnvironment(env);
    if (UserGroupInformation.isSecurityEnabled()) {
      ctx.setTokens(mAllTokens.duplicate());
    }

    try {
      LOG.info("Launching container {} for Alluxio worker on {} with worker command: {}",
          container.getId(), container.getNodeHttpAddress(), command);
      mNMClient.startContainer(container, ctx);
    } catch (Exception e) {
      LOG.error("Error launching container {}", container.getId(), e);
    }
  }

  private static Map<String, LocalResource> setupLocalResources(String resourcePath) {
  //  try {
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
     /* for (String resourceName : LOCAL_RESOURCE_NAMES) {
        localResources.put(resourceName, YarnUtils.createLocalResourceOfFile(
            new YarnConfiguration(), resourcePath));
      }*/
      return localResources;
   /* } catch (IOException e) {
      throw new RuntimeException("Cannot find resource", e);
    }*/
  }

  private static Map<String, String> setupMasterEnvironment() {
    return setupCommonEnvironment();
  }

  private static Map<String, String> setupWorkerEnvironment(String masterContainerNetAddress,
      int ramdiskMemInMB) {
    Map<String, String> env = setupCommonEnvironment();
    env.put("ALLUXIO_MASTER_HOSTNAME", masterContainerNetAddress);
    env.put("ALLUXIO_WORKER_MEMORY_SIZE","1");
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        env.put("ALLUXIO_USER", UserGroupInformation.getCurrentUser().getShortUserName());
      } catch (IOException e) {
        LOG.error("Get user name failed", e);
      }
    }
    return env;
  }

  private static Map<String, String> setupCommonEnvironment() {
    // Setup the environment needed for the launch context.
    Map<String, String> env = new HashMap<String, String>();
    env.put("ALLUXIO_HOME", ApplicationConstants.Environment.PWD.$());
    env.put("ALLUXIO_RAM_FOLDER", ApplicationConstants.Environment.LOCAL_DIRS.$());
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        env.put("ALLUXIO_USER", UserGroupInformation.getCurrentUser().getShortUserName());
      } catch (IOException e) {
        LOG.error("Get user name failed", e);
      }
    }
    return env;
  }
}
