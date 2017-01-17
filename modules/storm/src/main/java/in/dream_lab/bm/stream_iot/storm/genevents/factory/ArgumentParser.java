package in.dream_lab.bm.stream_iot.storm.genevents.factory;

import java.net.InetAddress;

/**
 * Created by tarun on 28/5/15.
 */
public class ArgumentParser {

    /*
    Convention is:
    Command Meaning: topology-fully-qualified-name <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id> <scaling-factor>
    Example command: SampleTopology L NA /var/tmp/bangalore.csv E01-01 0.001
     */
    public static ArgumentClass parserCLI(String [] args)
    {
    	if(args == null || args.length != 8){
            System.out.println("invalid number of arguments");
            return null;
        }
        else {
            ArgumentClass argumentClass = new ArgumentClass();
            argumentClass.setDeploymentMode(args[0]);
            argumentClass.setTopoName(args[1]);
            argumentClass.setInputDatasetPathName(args[2]);
            argumentClass.setExperiRunId(args[3]);
            argumentClass.setScalingFactor(Double.parseDouble(args[4]));
            argumentClass.setOutputDirName(args[5]);
            argumentClass.setTasksPropertiesFilename(args[6]);
            argumentClass.setTasksName(args[7]);
            return argumentClass;
        }
    }

    public static void main(String [] args){
        try {
        }catch(Exception e){
            e.printStackTrace();
        }
        ArgumentClass argumentClass = parserCLI(args);
        if(argumentClass == null){
            System.out.println("Improper Arguments");
        }
        else{
            System.out.println(argumentClass.getDeploymentMode() +" : " + argumentClass.getExperiRunId() + ":" + argumentClass.getScalingFactor());
        }
    }
}