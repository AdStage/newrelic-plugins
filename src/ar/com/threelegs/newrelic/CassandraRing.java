package ar.com.threelegs.newrelic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;

import ar.com.threelegs.newrelic.jmx.ConnectionException;
import ar.com.threelegs.newrelic.jmx.JMXHelper;
import ar.com.threelegs.newrelic.jmx.JMXTemplate;
import ar.com.threelegs.newrelic.util.CassandraHelper;

import com.newrelic.metrics.publish.Agent;
import com.newrelic.metrics.publish.util.Logger;
import com.typesafe.config.Config;

public class CassandraRing extends Agent {

	private static final Logger LOGGER = Logger.getLogger(CassandraRing.class);
	private String name;
	private Config config;

	public CassandraRing(Config config, String pluginName, String pluginVersion) {
		super(pluginName, pluginVersion);
		this.name = config.getString("name");
		this.config = config;
	}

	@Override
	public String getComponentHumanLabel() {
		return name;
	}

	@Override
	public void pollCycle() {
		LOGGER.debug("starting poll cycle");
		List<Metric> allMetrics = new ArrayList<Metric>();
		try {
			LOGGER.debug("getting ring hosts from discovery_host " + config.getString("discovery_host"));
			List<String> ringHosts = CassandraHelper.getRingHosts(config.getString("discovery_host"), config.getString("jmx_port"));

			LOGGER.debug("getting metrics for hosts [" + ringHosts + "]...");

			allMetrics.add(new Metric("Cassandra/global/totalHosts", "count", ringHosts.size()));
			int downCount = 0;

			for (final String host : ringHosts) {
				LOGGER.debug("getting metrics for host [" + host + "]...");

				try {
					List<Metric> metrics = JMXHelper.run(host, config.getString("jmx_port"), new JMXTemplate<List<Metric>>() {
						@Override
						public List<Metric> execute(MBeanServerConnection connection) throws Exception {

							ArrayList<Metric> metrics = new ArrayList<Metric>();

							// Latency
							Double rlMean = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "Mean");
							Double rlMax = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "Max");
							Double rlMin = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "Min");
							Double rl50th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "50thPercentile");
							Double rl75th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "75thPercentile");
							Double rl95th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "95thPercentile");
							Double rl98th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "98thPercentile");
							Double rl99th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "99thPercentile");
							Double rl999th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "999thPercentile");
							TimeUnit rlUnit = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Read", "LatencyUnit");
							rlMean = toMillis(rlMean, rlUnit);
              rlMax = toMillis(rlMax, rlUnit);
              rlMin = toMillis(rlMin, rlUnit);
              rl50th = toMillis(rl50th, rlUnit);
              rl75th = toMillis(rl75th, rlUnit);
              rl95th = toMillis(rl95th, rlUnit);
              rl98th = toMillis(rl98th, rlUnit);
              rl99th = toMillis(rl99th, rlUnit);
              rl999th = toMillis(rl999th, rlUnit);

							Double wlMean = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "Mean");
							Double wlMax = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "Max");
							Double wlMin = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "Min");
							Double wl50th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "50thPercentile");
							Double wl75th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "75thPercentile");
							Double wl95th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "95thPercentile");
							Double wl98th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "98thPercentile");
							Double wl99th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "99thPercentile");
							Double wl999th = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "999thPercentile");
							TimeUnit wlUnit = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.metrics", "Latency", "ClientRequest",
									"Write", "LatencyUnit");
							wlMean =  toMillis(wlMean,  wlUnit);
              wlMax =   toMillis(wlMax,   wlUnit);
              wlMin =   toMillis(wlMin,   wlUnit);
              wl50th =  toMillis(wl50th,  wlUnit);
              wl75th =  toMillis(wl75th,  wlUnit);
              wl95th =  toMillis(wl95th,  wlUnit);
              wl98th =  toMillis(wl98th,  wlUnit);
              wl99th =  toMillis(wl99th,  wlUnit);
              wl999th = toMillis(wl999th, wlUnit);

							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/MeanReads", "millis", rlMean));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/MaxReads", "millis", rlMax));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/MinReads", "millis", rlMin));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/50thReads", "millis", rl50th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/75thReads", "millis", rl75th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/95thReads", "millis", rl95th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/98thReads", "millis", rl98th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/99thReads", "millis", rl99th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/999thReads", "millis", rl999th));
							metrics.add(new Metric("Cassandra/global/Latency/MeanReads", "millis", rlMean));
							metrics.add(new Metric("Cassandra/global/Latency/MaxReads", "millis", rlMax));
							metrics.add(new Metric("Cassandra/global/Latency/MinReads", "millis", rlMin));
							metrics.add(new Metric("Cassandra/global/Latency/50thReads", "millis", rl50th));
							metrics.add(new Metric("Cassandra/global/Latency/75thReads", "millis", rl75th));
							metrics.add(new Metric("Cassandra/global/Latency/95thReads", "millis", rl95th));
							metrics.add(new Metric("Cassandra/global/Latency/98thReads", "millis", rl98th));
							metrics.add(new Metric("Cassandra/global/Latency/99thReads", "millis", rl99th));
							metrics.add(new Metric("Cassandra/global/Latency/999thReads", "millis", rl999th));

							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/MeanWrites", "millis", wlMean));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/MaxWrites", "millis", wlMax));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/MinWrites", "millis", wlMin));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/50thWrites", "millis", wl50th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/75thWrites", "millis", wl75th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/95thWrites", "millis", wl95th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/98thWrites", "millis", wl98th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/99thWrites", "millis", wl99th));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Latency/999thWrites", "millis", wl999th));
							metrics.add(new Metric("Cassandra/global/Latency/MeanWrites", "millis", wlMean));
							metrics.add(new Metric("Cassandra/global/Latency/MaxWrites", "millis", wlMax));
							metrics.add(new Metric("Cassandra/global/Latency/MinWrites", "millis", wlMin));
							metrics.add(new Metric("Cassandra/global/Latency/50thWrites", "millis", wl50th));
							metrics.add(new Metric("Cassandra/global/Latency/75thWrites", "millis", wl75th));
							metrics.add(new Metric("Cassandra/global/Latency/95thWrites", "millis", wl95th));
							metrics.add(new Metric("Cassandra/global/Latency/98thWrites", "millis", wl98th));
							metrics.add(new Metric("Cassandra/global/Latency/99thWrites", "millis", wl99th));
							metrics.add(new Metric("Cassandra/global/Latency/999thWrites", "millis", wl999th));

              //Throughput

							Integer rsActive = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.request", null, "ReadStage",
									null, "ActiveCount");
							Long rsCompleted = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.request", null, "ReadStage",
									null, "CompletedTasks");
							Long rsPending = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.request", null, "ReadStage",
									null, "PendingTasks");
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Request/ReadStage/ActiveCount", "count", rsActive));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Request/ReadStage/CompletedTasks", "count", rsCompleted));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Request/ReadStage/PendingTasks", "count", rsPending));
							metrics.add(new Metric("Cassandra/global/Request/ReadStage/ActiveCount", "count", rsActive));
							metrics.add(new Metric("Cassandra/global/Request/ReadStage/CompletedTasks", "count", rsCompleted));
							metrics.add(new Metric("Cassandra/global/Request/ReadStage/PendingTasks", "count", rsPending));

							Integer msActive = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.request", null, "MutationStage",
									null, "ActiveCount");
							Long msCompleted = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.request", null, "MutationStage",
									null, "CompletedTasks");
							Long msPending = JMXHelper.queryAndGetAttribute(connection, "org.apache.cassandra.request", null, "MutationStage",
									null, "PendingTasks");
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Request/MutationStage/ActiveCount", "count", msActive));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Request/MutationStage/CompletedTasks", "count", msCompleted));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Request/MutationStage/PendingTasks", "count", msPending));
							metrics.add(new Metric("Cassandra/global/Request/MutationStage/ActiveCount", "count", msActive));
							metrics.add(new Metric("Cassandra/global/Request/MutationStage/CompletedTasks", "count", msCompleted));
							metrics.add(new Metric("Cassandra/global/Request/MutationStage/PendingTasks", "count", msPending));

							// System
							Integer cpt = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Compaction", "name=PendingTasks"), "Value");
							Long mpt = JMXHelper.queryAndGetAttribute(connection, JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics",
									"type=ThreadPools", "path=internal", "scope=MemtablePostFlusher", "name=PendingTasks"), "Value");

							metrics.add(new Metric("Cassandra/hosts/" + host + "/Compaction/PendingTasks", "count", cpt));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/MemtableFlush/PendingTasks", "count", mpt));

							Integer dec = JMXHelper.queryAndGetAttribute(connection, JMXHelper.getObjectNameByKeys("org.apache.cassandra.net",
									"type=FailureDetector"), "DownEndpointCount");
							Integer uec = JMXHelper.queryAndGetAttribute(connection, JMXHelper.getObjectNameByKeys("org.apache.cassandra.net",
									"type=FailureDetector"), "UpEndpointCount");

							metrics.add(new Metric("Cassandra/global/DownEndpointCount", "count", dec));
							metrics.add(new Metric("Cassandra/global/UpEndpointCount", "count", uec));

							// Cache
							Double kchr = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Cache", "scope=KeyCache", "name=HitRate"),
									"Value");
							Long kcs = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Cache", "scope=KeyCache", "name=Size"),
									"Value");
							Integer kce = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Cache", "scope=KeyCache", "name=Entries"),
									"Value");
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Cache/KeyCache/HitRate", "rate", kchr));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Cache/KeyCache/Size", "bytes", kcs));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Cache/KeyCache/Entries", "count", kce));
							metrics.add(new Metric("Cassandra/global/Cache/KeyCache/HitRate", "rate", kchr));
							metrics.add(new Metric("Cassandra/global/Cache/KeyCache/Size", "bytes", kcs));
							metrics.add(new Metric("Cassandra/global/Cache/KeyCache/Entries", "count", kce));

							Double rchr = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Cache", "scope=RowCache", "name=HitRate"),
									"Value");
							Long rcs = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Cache", "scope=RowCache", "name=Size"),
									"Value");
							Integer rce = JMXHelper.queryAndGetAttribute(connection,
									JMXHelper.getObjectNameByKeys("org.apache.cassandra.metrics", "type=Cache", "scope=RowCache", "name=Entries"),
									"Value");
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Cache/RowCache/HitRate", "rate", rchr));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Cache/RowCache/Size", "bytes", rcs));
							metrics.add(new Metric("Cassandra/hosts/" + host + "/Cache/RowCache/Entries", "count", rce));
							metrics.add(new Metric("Cassandra/global/Cache/RowCache/HitRate", "rate", rchr));
							metrics.add(new Metric("Cassandra/global/Cache/RowCache/Size", "bytes", rcs));
							metrics.add(new Metric("Cassandra/global/Cache/RowCache/Entries", "count", rce));

							return metrics;
						}

						private Double toMillis(Double sourceValue, TimeUnit sourceUnit) {
							switch (sourceUnit) {
							case DAYS:
								return sourceValue * 86400000;
							case MICROSECONDS:
								return sourceValue * 0.001;
							case HOURS:
								return sourceValue * 3600000;
							case MILLISECONDS:
								return sourceValue;
							case MINUTES:
								return sourceValue * 60000;
							case NANOSECONDS:
								return sourceValue * 1.0e-6;
							case SECONDS:
								return sourceValue * 1000;
							default:
								return sourceValue;
							}
						}
					});

					if (metrics != null)
						allMetrics.addAll(metrics);
				} catch (ConnectionException e) {
					allMetrics.add(new Metric("Cassandra/downtime/hosts/" + e.getHost(), "value", 1));
					downCount++;
					allMetrics.add(new Metric("Cassandra/downtime/global", "count", downCount));
					e.printStackTrace();
				} catch (Exception e) {
					LOGGER.error(e);
				}
			}

		} catch (ConnectionException e) {
			allMetrics.add(new Metric("Cassandra/downtime/hosts/" + e.getHost(), "value", 1));
			// TODO: change to correct value (qty of failed connections) when we
			// make discoveryHosts a list.
			allMetrics.add(new Metric("Cassandra/downtime/global", "count", 1));
			LOGGER.error(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			LOGGER.debug("pushing " + allMetrics.size() + " metrics...");
			int dropped = 0;
			for (Metric m : allMetrics) {
				if (m.value != null && !m.value.toString().equals("NaN"))
					reportMetric(m.name, m.valueType, m.value);
				else
					dropped++;
			}
			LOGGER.debug("pushing metrics: done! dropped metrics: " + dropped);
		}
	}
}
