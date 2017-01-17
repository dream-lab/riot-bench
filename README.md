## RIoTBench: A Real-time IoT Benchmark for Distributed Stream Processing Platforms
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

 <!---### Application  benchmarks 		
 | App. Name  | Code |		
 | ------------- | ------------- |		  | ------------- | ------------- |
 | Extraction, Transform and Load | ETL   |		 +| XML Parsing  | Parse   |
 | Satistical Summarization dataflow  | STATS   |		 +| Bloom Filter  | Filter   |
 | Model Training  | TRAIN   |		 +| Average   | Statistical   |
 | Predictive Analytics   | PRED   |
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

Please refer the paper for detailed info  - <arxiv-link> 





