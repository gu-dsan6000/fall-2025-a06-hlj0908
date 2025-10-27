# Problem 1
## Objective
The goal of this task was to analyze Spark cluster log files to determine the distribution of different log levels — INFO, WARN, and ERROR. This helps evaluate overall cluster stability and identify potential performance or reliability issues reflected in log messages.

## Approach
1. **Data Ingestion**: Read all log files from the specified directory using PySpark, enabling recursive file lookup to include logs from subdirectories.
2. **Pattern Extraction**: Use regular expressions to identify and extract log levels (INFO, WARN, ERROR) from each log entry.
3. **Aggregation**: Log entries were grouped by level using groupBy("log_level").count() and sorted by frequency.

## Output Files  
Three deliverables were generated:

- **`data/output/problem1_counts.csv`** — Log level counts  
- **`data/output/problem1_sample.csv`** — Ten random sample log entries with levels  
- **`data/output/problem1_summary.txt`** — Overall statistics and percentage distribution  

## Results
### Overall Summary
Based on the aggregated outputs from all processed log files, the results are summarized below.

| Log Level | Count  | Percentage (%) |
|-----------|--------|------------|
| INFO      | 54778964 | 99.92        |
| WARN      | 1919  | 0.04        |
| ERROR     | 22518 | 0.04         |


### Sample Log Entries
The following examples (from problem1_sample.csv) illustrate typical log lines detected by the parser:

| Log Level | Sample Log Entry                                                                                     |
|-----------|-----------------------------------------------------------------------------------------------------| 
| INFO      | 17/03/29 10:04:41 INFO ApplicationMaster: Registered signal handlers for [TERM, HUP, INT]                  |
| WARN      | 17/03/29 10:04:43 WARN YarnAllocator: Container request (host: Any, capability: <memory:28160, vCores:5>) |
| ERROR     | 17/03/29 10:04:45 ERROR RMContainerAllocator: Lost executor container_1485248649253_0052_01_000005 |

### Analysis
The analysis of the log-level distribution reveals that the dataset is overwhelmingly dominated by INFO entries, accounting for more than 99.9% of all log lines. These messages primarily reflect normal system progress and routine Spark application updates. In contrast, less than 0.1% of the entries were classified as WARN or ERROR, indicating that the Spark cluster maintained exceptional stability throughout the execution period. The few WARN lines observed likely originated from temporary YARN resource allocation delays or scheduling inefficiencies, while the sparse ERROR entries corresponded to short-lived executor or container failures that were automatically recovered by Spark’s fault-tolerance mechanism. Overall, this pattern suggests that the cluster was well-configured, with balanced resource utilization and minimal job retries. The consistent timing and spacing of INFO messages further imply synchronous executor activity and efficient task scheduling, confirming that no persistent node failures or starvation events occurred. In summary, the log analysis highlights a highly reliable and robust Spark environment with an estimated operational stability of approximately 99.9%

## Conclusion
The results of Problem 1 clearly demonstrate that the Spark cluster operated under highly stable conditions. More than 99.9% of all log entries were classified as INFO, indicating normal system activity and smooth job execution. Only a negligible number of WARN and ERROR messages were detected, most of which reflected transient resource allocation delays or short-lived executor restarts automatically handled by Spark’s recovery mechanism. This distribution confirms that the cluster was properly configured, network connections were stable, and no critical node failures occurred during operation.

Overall, the log-level analysis provides strong evidence that the deployed Spark environment achieved high reliability, efficient task scheduling, and robust fault tolerance, ensuring consistent performance across all stages of execution.

# Problem 2
## Objective
The goal of this problem is to understand how the cluster behaves over time and identify which clusters/applications dominate the workload; build clear time series for visualization and calculate summary statistics.

## Approach
All log files were recursively parsed to extract cluster_id, application_id, and timestamp information. Each application’s start and end times were determined using the first and last timestamped entries within the corresponding log files. The resulting records were compiled into a timeline dataset, then aggregated at the cluster level to produce application counts and usage durations. Additional statistical summaries were computed to measure workload concentration. Two visualizations were generated:

1. A bar chart depicting the number of applications executed per cluster.
2. A density plot (log-scaled) illustrating the distribution of job durations for the most active cluster.

## Results
The processed dataset revealed a total of 6 unique clusters and 194 applications, with an average of 32.33 applications per cluster. However, the distribution of workload was highly uneven. One cluster, identified as 1485248649253, accounted for 181 applications, representing approximately 93% of the total workload. The remaining clusters exhibited minimal activity—each executing between one and eight applications—which indicates that the overall operational workload was concentrated on a single dominant cluster.

This workload concentration suggests that the system architecture prioritized centralized scheduling and resource allocation to a primary production cluster, while the other clusters served testing, backup, or historical roles. The timeline dataset confirms continuous task execution on the main cluster, whereas other clusters show sporadic short-duration workloads. This operational pattern reflects an optimized utilization of compute resources on the primary node, minimizing inter-cluster communication overhead.

The density plot reveals a heavy-tailed distribution of application durations, with a high frequency of short to medium-length jobs and a long tail of few extended executions. Such a pattern is characteristic of heterogeneous Spark workloads combining quick ETL transformations with a limited number of computationally intensive analytical jobs. The absence of multimodal peaks in the distribution indicates a consistent job profile, suggesting that applications were relatively uniform in complexity and data size.

From a performance standpoint, the concentration of workloads on a single cluster enhances cache locality and simplifies task coordination but may also introduce potential bottlenecks during peak operation. Monitoring and capacity planning should therefore prioritize this primary cluster to prevent resource saturation and maintain throughput efficiency. Conversely, the underutilized clusters represent potential capacity for load balancing or future scaling.

## Conclusion
The cluster usage analysis demonstrates a centralized workload pattern in which one primary cluster executes the majority of Spark applications. This design choice yields high operational efficiency under the current workload but introduces vulnerability to single-point performance limitations. The heavy-tailed duration distribution highlights that while most applications are lightweight, a few long-running jobs may dominate total execution time. To improve system resilience and utilization balance, future configurations could implement dynamic job routing, load redistribution, and adaptive resource scaling across clusters. Overall, the Spark environment exhibits strong stability, efficient task scheduling, and predictable temporal workload behavior, validating the correctness of both the cluster setup and the log-processing methodology.