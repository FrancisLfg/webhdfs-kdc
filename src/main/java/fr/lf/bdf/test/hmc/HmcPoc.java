package fr.lf.bdf.test.hmc;

import com.github.sakserv.minicluster.MiniCluster;
import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.KdcLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

/**
 * POC that shows how to use hadoop-mini-cluster with MiniHdfs + MiniKdc
 * with a SWebHdfsFileSystem client.
 * Directly inspired from test classes of
 * @see com.github.sakserv.minicluster.impl.KdcLocalCluster
 *
 * @see <a href="https://github.com/sakserv/hadoop-mini-clusters">hadoop-mini-cluster git project</a>
 */
public class HmcPoc {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HmcPoc.class);

    private static final String KEY = "[CHECK-LOG] - {}";

    private static KdcLocalCluster kdcLocalCluster;
    private static HdfsLocalCluster hdfsLocalCluster;

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error(KEY, "unable to load property file", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    public static void main(String... args) {

        List clusters = null;
        try {
            clusters = prepareClusters();
        } catch (Exception e) {
            LOG.error(KEY, "unable to prepare/start clusters", e);
            return;
        }

        try {
            doSomeStuff();
        } catch (IOException | InterruptedException e) {
            LOG.error(KEY, "unable to perform op on clusters", e);
        } finally {
            stopAll(clusters);
        }
    }

    private static void doSomeStuff() throws IOException, InterruptedException {

        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

        SWebHdfsFileSystem client = ugi.doAs((PrivilegedExceptionAction<SWebHdfsFileSystem>) () -> {
            String uri = "swebhdfs://" + hdfsLocalCluster.getHdfsConfig().get("dfs.namenode.https-address");
            return (SWebHdfsFileSystem) FileSystem.get(new URI(uri), hdfsLocalCluster.getHdfsConfig());
        });

        // Write a file to HDFS containing the client string
        FSDataOutputStream writer = client.create(
                new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to client string
        FSDataInputStream reader = client.open(
                new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
        String res = reader.readUTF();
        LOG.info(KEY, res);
        reader.close();
        if (!res.equals(propertyParser.getProperty(ConfigVars.HDFS_TEST_STRING_KEY))) {
            LOG.error(KEY, "file content is different from expected string");
            return;
        }

        UserGroupInformation.getCurrentUser().logoutUserFromKeytab();
        UserGroupInformation.reset();

        // Read again, should throw AccessControlException
        try {
            FSDataInputStream readerNa = client.open(
                    new Path(propertyParser.getProperty(ConfigVars.HDFS_TEST_FILE_KEY)));
            String resNa = readerNa.readUTF();
            LOG.info(KEY, resNa);
            readerNa.close();
            LOG.error(KEY, "should not be able to read, still authenticated ?");
        } catch (AccessControlException e) {
            LOG.info(KEY, "not authenticated, as expected !");
        } catch (Exception e) {
            LOG.error(KEY, "this exception was not expected ...");
        }
    }

    private static List<MiniCluster> prepareClusters() throws Exception {
        // KDC
        kdcLocalCluster = new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
        kdcLocalCluster.start();
        Configuration baseConf = kdcLocalCluster.getBaseConf();

//        //HDFS
        Configuration hdfsConfig = new HdfsConfiguration();
        hdfsConfig.addResource(baseConf);
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsNamenodeHttpPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)))
                .setHdfsConfig(hdfsConfig)
                .build();
        hdfsLocalCluster.start();
        return Arrays.asList(hdfsLocalCluster, kdcLocalCluster);
    }

    private static void stopAll(List<MiniCluster> clusters) {
        for (MiniCluster node : clusters) {
            try {
                node.stop();
            } catch (Exception e) {
                LOG.error(KEY, "unable to stop a node", e);
            }
        }
    }

}
