# riot-bench
## IoT Benchmark using Apache Storm [VLDB/TPCTC-2016]
### IoT  Micro-benchmarks 
| Task Name  | Category |
| ------------- | ------------- |
| XML Parsing  | Parse   |
| Bloom Filter  | Filter   |
| Average   | Statistical   |
|   Distinct Appox. Count  | Statistical   |
|   Kalman Filter | Statistical   |
|   Second Order Moment | Statistical   |
|   Decision Tree Classify | Predictive   |
|   Multi-variate Linear Reg. | Predictive   |
|   Sliding Linear Regression | Predictive   |
|   Azure Blob D/L | IO   |
|   Azure Blob U/L | IO   |
|   Azure Table Query | IO   |
|   MQTT Publish | IO   |

### Application  benchmarks 
| App. Name  | Code |
| ------------- | ------------- |
| Pre-processing & statistical summarization dataflow  | STATS   |
| Predictive Analytics dataflow   | PRED   |


<!--- ![FCAST](https://github.com/anshuiisc/FIG/blob/master/STATS-and-PRED.png)  --->

- Steps to run benchmark's
- Once cloned  run 
    ```
   mvn clean compile package -DskipTests
    ```
- To submit jar microbenchmarks- 
 ```
 storm jar <stormJarPath>   in.dream_lab.bm.stream_iot.storm.topo.micro.MicroTopologyDriver  C  <microTaskName>  <inputDataFilePath used by CustomEventGen and spout>   PLUG-<expNum>  <rate as 1x,2x>  <outputLogPath>   <tasks.properties File Path>   <TopoName>
 
 
 ```
- For microTaskName please refer  switch logic in  MicroTopologyFactory class in package   "in.dream_lab.bm.stream_iot.storm.topo.micro"   

Please refer the paper for detailed info  - <http://arxiv.org/abs/1606.07621> 





