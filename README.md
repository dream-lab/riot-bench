## RIoTBench: A Real-time IoT Benchmark for Distributed Stream Processing Platforms
### IoT  Micro-benchmarks 
| Task Name  | Code | Category |Pattern|Selectivity|State
| -----------| ------------- | ------------- | ------------- |------------- |
|Annotate | ANN | Parse |  Transform |1:1|No|
|CsvToSenML | C2S | Parse |  Transform |1:1|No|
### Application  benchmarks 
| App. Name  | Code |
| ------------- | ------------- |
| Extraction, Transform and Load | ETL   |
| Satistical Summarization dataflow  | STATS   |
| Model Training  | TRAIN   |
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





