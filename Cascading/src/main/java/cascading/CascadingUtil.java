package cascading;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import riffle.process.Process;
import cascading.cascade.Cascade;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.tez.Hadoop2TezFlowConnector;

import com.google.common.collect.Lists;

/**
 * Helper class providing the following:
 * 
 * 1. Adding Cascading and other dependent jars to the Hadoop distributed cache.
 * This requires that Cascading jars are all on the client classpath when the
 * application is executed. Note: a simpler alternative is to build a "fat jar",
 * containing all dependent jars in /lib within the jar file. This can be easily
 * done in a Gradle build
 * 
 */
public abstract class CascadingUtil {

  private static Logger logger = LoggerFactory.getLogger(CascadingUtil.class);

  public static Properties addDependencyJars(Configuration conf,
      Class<?>... classes) {
    // Add standard Cascading library dependencies
    String jgrapht = ClassUtil.findContainingJar(Graph.class);
    String riffle = ClassUtil.findContainingJar(Process.class);
    String janino = ClassUtil.findContainingJar(Java.class);
    String cascadingCore = ClassUtil.findContainingJar(Cascade.class);
    String cascadingHadoop = ClassUtil.findContainingJar(HadoopFlow.class);
    String commonsCompiler = ClassUtil
        .findContainingJar(CompileException.class);
    String cascadingHadoop2 = ClassUtil
        .findContainingJar(Hadoop2MR1FlowConnector.class);
    String cascadingTez = ClassUtil
        .findContainingJar(Hadoop2TezFlowConnector.class);

    assert jgrapht != null;
    assert riffle != null;
    assert janino != null;
    assert cascadingCore != null;
    assert cascadingHadoop != null;
    assert commonsCompiler != null;
    assert cascadingHadoop2 != null;
    assert cascadingTez != null;

    List<String> jars = Lists.newArrayList(jgrapht, riffle, janino,
        cascadingCore, cascadingHadoop, cascadingHadoop2, commonsCompiler,
        cascadingTez);

    for (Class<?> clazz : classes) {
      String jar = ClassUtil.findContainingJar(clazz);
      assert jar != null;
      jars.add(jar);
    }

    logger.info("Adding jars [{}] to distributed cache", jars);

    try {
      new GenericOptionsParser(conf, new String[] { "-libjars",
          StringUtils.join(jars, ',') });
      return convertConfigurationToProperties(conf);
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static Properties convertConfigurationToProperties(Configuration conf) {
    Properties props = new Properties();

    if (conf != null) {
      for (Map.Entry<String, String> entry : conf) {
        props.setProperty(entry.getKey(), entry.getValue());
      }
    }

    return props;
  }
}