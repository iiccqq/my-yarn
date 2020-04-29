package alluxio.yarn;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public final class Client {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private YarnClient mYarnClient;
  /** Yarn configuration. */
  private YarnConfiguration mYarnConf = new YarnConfiguration();
  /** Container context to launch application master. */
  private ContainerLaunchContext mAmContainer;
  /** ApplicationMaster specific info to register a new Application. */
  private ApplicationSubmissionContext mAppContext;
  /** Application name. */
  private String mAppName;
  /** ApplicationMaster priority. */
  private int mAmPriority;
  /** Queue for ApplicationMaster. */
  private String mAmQueue;
  /** Amount of memory to request for running the ApplicationMaster. */
  private int mAmMemoryInMB;
  /** Number of virtual cores to request for running the ApplicationMaster. */
  private int mAmVCores;
  /** ApplicationMaster jar file on HDFS. */
  private String mResourcePath = "hdfs://172.16.12.26:8020/user/root";
  /** Number of Alluxio workers. */
  private int mNumWorkers;
  /** Address to run Alluxio master. */
  private String mMasterAddress;
  /** Maximum number of workers to allow on a single host. */
  private int mMaxWorkersPerHost;
  /** Id of the application. */
  private ApplicationId mAppId;
  /** Command line options. */
  private Options mOptions;
  private Configuration mAlluxioConf;

  /**
   * Constructs a new client for launching an Alluxio application master.
   *
   * @param alluxioConf Alluxio configuration
   */
  public Client(Configuration alluxioConf) {
    mAlluxioConf = alluxioConf;
    mOptions = new Options();
    mOptions.addOption("appname", true, "Application Name. Default 'Alluxio'");
    mOptions.addOption("priority", true, "Application Priority. Default 0");
    mOptions.addOption("queue", true,
        "RM Queue in which this application is to be submitted. Default 'default'");
    mOptions.addOption("am_memory", true,
        "Amount of memory in MB to request to run ApplicationMaster. Default 1024");
    mOptions.addOption("am_vcores", true,
        "Amount of virtual cores to request to run ApplicationMaster. Default 1");
    mOptions.addOption("resource_path", true,
        "(Required) HDFS path containing the Application Master");
    mOptions.addOption("alluxio_home", true,
        "(Required) Path of the home dir of Alluxio deployment on YARN slave machines");
    mOptions.addOption("master_address", true, "(Required) Address to run Alluxio master");
    mOptions.addOption("help", false, "Print usage");
    mOptions.addOption("num_workers", true, "Number of Alluxio workers to launch. Default 1");
  }

  /**
   * Constructs a new client for launching an Alluxio application master and
   * parses command line options.
   *
   * @param args Command line arguments
   * @param alluxioConf Alluxio configuration
   * @throws ParseException if an error occurs when parsing the argument
   */
  public Client(String[] args, Configuration alluxioConf) throws ParseException {
    this(alluxioConf);
    parseArgs(args);
  }

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    try {
      Configuration config = new Configuration();
      Client client = new Client(config);
      System.out.println("Initializing Client");
      if (!client.parseArgs(args)) {
        System.out.println("Cannot parse commandline: " + Arrays.toString(args));
        System.exit(0);
      }
      System.out.println("Starting Client");
      client.run();
    } catch (Exception e) {
      System.err.println("Error running Client " + e);
      System.exit(1);
    }
  }

  /**
   * Main run function for the client.
   */
  public void run() throws IOException, YarnException {
    submitApplication();
  }

  /**
   * Helper function to print out usage.
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", mOptions);
  }

  /**
   * Parses command line options.
   *
   * @param args Parsed command line options
   * @return Whether the parseArgs was successful to run the client
   * @throws ParseException if an error occurs when parsing the argument
   */
  private boolean parseArgs(String[] args) throws ParseException {
   // Preconditions.checkArgument(args.length > 0, "No args specified for client to initialize");
   // CommandLine cliParser = new GnuParser().parse(mOptions, args);
/*
    Preconditions.checkArgument(mAmMemoryInMB > 0,
        "Invalid memory specified for application master, " + "exiting. Specified memory="
            + mAmMemoryInMB);
    Preconditions.checkArgument(mAmVCores > 0,
        "Invalid virtual cores specified for application master, exiting."
            + " Specified virtual cores=" + mAmVCores);

 */
    return true;
  }

  /**
   * Submits an application to the ResourceManager to run ApplicationMaster.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for creating
   * applications and setting up the application submission context. This was not available in the
   * alpha API.
   */
  private void submitApplication() throws YarnException, IOException {
    // Initialize a YarnClient
    mYarnClient = YarnClient.createYarnClient();
    mYarnClient.init(mYarnConf);
    mYarnClient.start();

    // Create an application, get and check the information about the cluster
    YarnClientApplication app = mYarnClient.createApplication();
    // Get a response of this application, containing information of the cluster
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    // Check if the cluster has enough resource to launch the ApplicationMaster
    checkClusterResource(appResponse);

    // Check that there are enough hosts in the cluster to support the desired number of workers
   //checkNodesAvailable();

    // Set up the container launch context for the application master
    mAmContainer = Records.newRecord(ContainerLaunchContext.class);
    setupContainerLaunchContext();

    // Finally, set-up ApplicationSubmissionContext for the application
    mAppContext = app.getApplicationSubmissionContext();
    setupApplicationSubmissionContext();

    // Submit the application to the applications manager.
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    mAppId = mAppContext.getApplicationId();
    System.out.println("Submitting application of id " + mAppId + " to ResourceManager");
    mYarnClient.submitApplication(mAppContext);
    monitorApplication();
  }

  // Checks if the cluster has enough resource to launch application master,
  // alluxio master and alluxio workers
  private void checkClusterResource(GetNewApplicationResponse appResponse) {
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
  }

  // Checks that there are enough nodes in the cluster to run the desired number of workers
  private void checkNodesAvailable() throws YarnException, IOException {
    Set<String> hosts = YarnUtils.getNodeHosts(mYarnClient);
    Preconditions.checkArgument(mNumWorkers <= hosts.size() * mMaxWorkersPerHost,
        "Not enough nodes in cluster to support specified number of workers, " + String.format(
            "specified=%s, but there are only %d usable hosts and %d workers allowed per host: %s",
            mNumWorkers, hosts.size(), mMaxWorkersPerHost, hosts));
  }

  public static String buildCommand(Map<String, String> args) {
    CommandBuilder commandBuilder =
        new CommandBuilder("./start.sh");
    for (Map.Entry<String, String> argsEntry : args.entrySet()) {
      commandBuilder.addArg(argsEntry.getKey(), argsEntry.getValue());
    }
    // Redirect stdout and stderr to yarn log files
    commandBuilder.addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    commandBuilder.addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
    return commandBuilder.toString();
  }

  private void setupContainerLaunchContext() throws IOException, YarnException {

    final String amCommand = buildCommand(new HashMap<>());
    System.out.println("ApplicationMaster command: " + amCommand);
    mAmContainer.setCommands(Collections.singletonList(amCommand));

    // Setup local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put("start.sh",
        YarnUtils.createLocalResourceOfFile(mYarnConf, mResourcePath + "/start.sh"));
    localResources.put("am.jar",
        YarnUtils.createLocalResourceOfFile(mYarnConf, mResourcePath + "/am.jar"));
    mAmContainer.setLocalResources(localResources);

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    mAmContainer.setEnvironment(appMasterEnv);

    // Set up security tokens for launching our ApplicationMaster container.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = mYarnConf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
      }
      org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(mYarnConf);
      // getting tokens for the default file-system.
      final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      // getting yarn resource manager token
      org.apache.hadoop.conf.Configuration config = mYarnClient.getConfig();
      Token<TokenIdentifier> token = ConverterUtils.convertFromYarn(
          mYarnClient.getRMDelegationToken(new org.apache.hadoop.io.Text(tokenRenewer)),
          ClientRMProxy.getRMDelegationTokenService(config));
      LOG.info("Added RM delegation token: " + token);
      credentials.addToken(token.getService(), token);

      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer buffer = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      mAmContainer.setTokens(buffer);
    }
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) throws IOException {
    appMasterEnv.put("MY_ENV_USER", "fc");
  }

  /**
   * Sets up the application submission context.
   */
  private void setupApplicationSubmissionContext() {
    // set the application name
    mAppContext.setApplicationName(mAppName);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and vcores requirements
    Resource capability = Resource.newInstance(mAmMemoryInMB, mAmVCores);
    mAppContext.setResource(capability);

    // Set the queue to which this application is to be submitted in the RM
    mAppContext.setQueue(mAmQueue);

    // Set the AM container spec
    mAppContext.setAMContainerSpec(mAmContainer);

    // Set the priority for the application master
    mAppContext.setPriority(Priority.newInstance(mAmPriority));
  }

  /**
   * Monitor the submitted application until app is running, finished, killed or failed.
   */
  private void monitorApplication() throws YarnException, IOException {
    while (true) {
      // Check app status every 5 seconds
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      // Get application report for the appId we are interested in
      ApplicationReport report = mYarnClient.getApplicationReport(mAppId);

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      switch (state) {
        case RUNNING:
          System.out.println("Application is running. Tracking url is " + report.getTrackingUrl());
          return;
        case FINISHED:
          if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
            System.out.println("Application has completed successfully");
          } else {
            System.out.println("Application finished unsuccessfully. YarnState="
                + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
          }
          return;
        case KILLED: // intended to fall through
        case FAILED:
          System.out.println("Application did not finish. YarnState=" + state.toString()
              + ", DSFinalStatus=" + dsStatus.toString());
          return;
        default:
          System.out.println("Application is in state " + state + ". Waiting.");
      }
    }
  }
}
