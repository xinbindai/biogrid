package org.xdai.biogrid;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
//import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
//import java.util.Map;

public class MasterNode {
	
	private String cmd;
	private int slicepos;
	private int maxFailedTimes=0;

	private ArrayList<ComputingNode> computingnodes;
	private ArrayList<ComputingThread> computingthreads;
	
	private String workdir;
	private String syncdirs;
	private boolean allowfilesync;
	private String[] ssh_parameters;
	public static String ssh_params[] = {"-q", "-o", "PasswordAuthentication=no", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"};
	
    public static void ActivateAllComputingThreads(MasterNode mn){
    	for (ComputingThread computingthread:mn.getComputingthreads()) {
    		computingthread.setActive(true);
    	}
    }
    
	public boolean dispatch(SliceManager sm){
		boolean atLeastOneNodeIsActive = false;
		for (ComputingThread computingthread:computingthreads) {
			if(!computingthread.isActive()||computingthread.getComputingnode().disable) continue;
			atLeastOneNodeIsActive = true;
			if(computingthread.getStatus()!=Status.READY) continue;
			
			Slice slice;
			if(sm.isGroupmode()) {
				slice=sm.getReadySlice(computingthread.getComputingnode().getGroupID(), computingnodes.size());  //group ID of total nodes
				if(slice==null) slice=sm.getReadySlice();
			}else{
				slice=sm.getReadySlice();
			}
			
			if(slice==null){
				//System.err.print("Failed to find unanalyzed slice to dispatch");
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
				/*int runningslicenum=sm.getSliceCount(Status.RUNNING);
				int runningctnum=this.getRunningComputingThreadNum();
				if(runningslicenum!=runningctnum){
					System.err.println("Unmatched error: All slices has been submitted for running. However, there are "
	                      +runningslicenum + " running slices and " + runningctnum +" running computingthreads"
				    );
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {}
					//System.exit(-1);
				}*/
				return atLeastOneNodeIsActive;
			}
			computingthread.bindSlice(slice);  //also set starttime
			try{
			    Thread thr = new Thread(computingthread);
			    thr.start();
			}catch(java.lang.Throwable e){
				System.err.println( "\n********* Failed to open thread for slice #"+computingthread.getCurslice().getId()+" with computing thread " 
		                  + computingthread.getDescStr() + ", error message: ***********\n"
	                      + e.getMessage()
	                      );
			    System.err.println("Recycle the slice #" + computingthread.getCurslice().getId());
			    System.err.println("Release computing thread " + computingthread.getDescStr() + " for other tasks");
			    computingthread.setActive(false);
				computingthread.releaseSlice(Status.READY);
			}
		}
		return atLeastOneNodeIsActive;
	}
	
	public static void killStuckJobs(MasterNode mn, SliceManager sm, int uplimitrunningtime){
		
		int minimumSliceNumerForCheck=(int)(sm.getSliceTotalNumber()*0.1);
		if(minimumSliceNumerForCheck<5)  minimumSliceNumerForCheck=5;
		if(sm.getFinishedslicesnum()<=minimumSliceNumerForCheck) return;  //check stuck jobs when progress is 
		
		long averageslicetime=sm.getAveragetime();
		
        for(ComputingThread ct : mn.getComputingthreads()){
        	
        	if(ct.getStatus()!=Status.RUNNING) continue;
        	
        	long elapsedtime=ct.getElapsedTimeInMilliSecs();
        	
        	if(elapsedtime>averageslicetime*uplimitrunningtime){
        		//System.err.println("Routine check: try to kill stuck threads "+ ct.getDescStr());
    			String ss = "Kill job on computing thread " + ct.getDescStr() 
    					+ ". It has run for " + elapsedtime + " milliseconds over average "
    					+ averageslicetime + " milliseconds\n";
    			try {
    			    ct.getRp().setMessage(ss);
   				    ct.getRp().getProcess().destroy();
    			}catch(Exception e) {}
        	}
        }
	}
	
	private void initComputingThreads(){
    	computingthreads=new ArrayList<ComputingThread>();
    	for(ComputingNode computingnode: computingnodes){
           for(int i=0;i<computingnode.getTotalNumberOfThreads();i++){
        	   ComputingThread tmp = new ComputingThread(computingnode, i, cmd, slicepos, maxFailedTimes);
        	   computingnode.getThreads().add(tmp);
        	   computingthreads.add(tmp);
           }
    	}
    	Collections.shuffle(computingthreads);
    }
	
	/*
	private void initComputingNodes(ArrayList<Map.Entry<String, Integer>> nodelist){
		computingnodes=new ArrayList<ComputingNode>();
		int groupIDOfNodes=0;
		for(Map.Entry<String, Integer> tmp: nodelist){
			String hostname=tmp.getKey();
			int threadnum = tmp.getValue();
			ComputingNode tmpcn = new ComputingNode(this, hostname, threadnum, groupIDOfNodes, workdir);
			computingnodes.add(tmpcn);
			groupIDOfNodes++;
		}
	}*/

	private void initComputingNodesByConfig(String filename, double cpufactor){
		String[] def_ssh_parameters = Tools.mergeStringArray(MasterNode.ssh_params, this.ssh_parameters);
		computingnodes = new ArrayList<ComputingNode>();
		int groupIDOfNodes=0;

		if (filename == null || filename.equals("")) {
			//AbstractMap.SimpleEntry<String, Integer> as = new AbstractMap.SimpleEntry<String, Integer>(Tools.LOCALHOSTIP, Tools.getLocalProcessors());
			int threadnum = (int)(Tools.getLocalProcessors() * cpufactor);
			ComputingNode tmpcn = new ComputingNode(this, Tools.LOCALHOSTIP, threadnum, groupIDOfNodes, workdir, null, null);
			computingnodes.add(tmpcn);
			groupIDOfNodes++;
			System.err.println("  Automatically add " + threadnum + " local node(s)");
		} else {
			try {
				BufferedReader in = new BufferedReader(new FileReader(filename));
				String str;
				int totalcpus=0;
				while ((str = in.readLine()) != null) {
					if (str.matches("^\\s*$") || str.matches("^\\s*#.*$"))	continue; // skip empty and comment line
					str = str.replaceAll("^\\s+|\\s+$", ""); // trim
					String[] res = str.split("[\t ]+"); // split
					if (res.length <= 1) continue; // if there is no two cols, skip.
					if (res.length > 2) {
						String[] res2 = str.split("[\t]"); // re-split by tab to take more options(colume 3 is ssh-options)
						if( res2.length == 3) res = res2;
					}
					
					String username = null;
					String hostname = null;
					res[0] = res[0].trim();
				    if(Tools.isLocalHost(res[0])) {
						hostname =Tools.LOCALHOSTIP; 
					} else {
						String[] user_hostname = res[0].split("@");
						if(user_hostname.length==1){
							hostname = user_hostname[0];
						} else if(user_hostname.length==2){
							username = user_hostname[0];
							hostname = user_hostname[1];
						} else{
							System.out.println("Skip "+ res[0] +". Hostname format error. The node won't be added into cluster.");
							continue;
						}
					}
					
					String[] node_ssh_paramater = null;
					if(res.length==3){
						node_ssh_paramater = Tools.mergeStringArray(def_ssh_parameters, res[2].trim().split("[\t ]+"));
					}else{
						node_ssh_paramater = def_ssh_parameters;
					}

					double paracpus = 0.0; // parameter for # of CPUs in config file, effective format: 0 / 12 / 0.25
					try {
						paracpus = Double.parseDouble(res[1].trim());
					} catch (Exception ex) {
						System.err.println("Invalid CPU number for " + hostname + ", node config is: " + str);
						continue;
					}

					int finalcpus = 0; // the final cpu number based on current situation and config file.
					if (paracpus >= 1.0) {
						finalcpus = (int) paracpus;
					}else{
					    int detectedcpus = 0; // real # of processor in computing node
					    if(Tools.isLocalHost(hostname)) {
						  detectedcpus = Tools.getLocalProcessors();
					    } else {
						  //detectedcpus = Tools.getRemoteProcessors(res[0]);
						  detectedcpus = Tools.getRemoteProcessors(hostname, username, node_ssh_paramater);
					    }

					    if (paracpus <= 0.0) {
						  finalcpus = detectedcpus;
					    }else {
						  finalcpus = (int) (paracpus * detectedcpus + 0.5);			  // if(cpus==0) cpus=1;
					    }
					}
					finalcpus=(int)(finalcpus*cpufactor);
					if(finalcpus>0){
						//AbstractMap.SimpleEntry<String, Integer> as = new AbstractMap.SimpleEntry<String, Integer>(res[0], finalcpus);
						//v.add(as);
						//ComputingNode(MasterNode masternode, String hostname, int totalNumberOfThreads, int groupID, String workdir)
						ComputingNode tmpcn = new ComputingNode(this, hostname, finalcpus, groupIDOfNodes, workdir, username, node_ssh_paramater);
						computingnodes.add(tmpcn);
						groupIDOfNodes++;
						totalcpus=totalcpus+finalcpus;
					}
					if (computingnodes.size() > 1) System.err.print(","); // don't print comma for the first node computer
					if(username!=null&&!username.equals(""))	System.err.print("  " + finalcpus + " nodes " + username + "@" + hostname);
					else                System.err.print("  " + finalcpus + " nodes " + hostname);
				}
				System.err.println();
				System.err.println("TOTAL: " + totalcpus + " nodes");
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}


	private void mysync() throws Exception{
		if (!syncdirs.equals("")) {
			System.err.print("Sync public directories to nodes......");
			SyncDirs sd = new SyncDirs(syncdirs, computingnodes, allowfilesync);
			sd.dosync();
			System.err.println("......done.");
		} else {
			System.err.println("Skip sync step......");
		}
	}
	
	
	public static MasterNode MasterNodeFactory(String nodeconfigfn, double nodefactor, String workdir, String syncdirs, boolean allowfilesync,
			String cmd, int slicepos, int maxFailedTimes, String ssh_parameters_str) throws Exception {

		MasterNode masternode = new MasterNode();
		masternode.workdir=workdir;
		masternode.syncdirs=syncdirs;
		masternode.allowfilesync=allowfilesync;
		masternode.cmd=cmd;
		masternode.slicepos=slicepos;
		masternode.maxFailedTimes=maxFailedTimes;
		masternode.ssh_parameters = ssh_parameters_str.split("[\t ]+");  //ssh_parameters_str parsed from command line input


		masternode.initComputingNodesByConfig(nodeconfigfn, nodefactor);
		masternode.mysync();
    	for(ComputingNode computingnode: masternode.computingnodes){
    		if(!computingnode.disable) computingnode.makeWorkdir(); 
    	}
		masternode.initComputingThreads();
		
		return masternode;
	}

	public  ArrayList<ComputingThread> getComputingthreads() {
		return computingthreads;
	}

	public  ArrayList<ComputingNode> getComputingnodes() {
		return computingnodes;
	}
	
	public int getRunningComputingThreadNum(){
		int num=0;
		for(ComputingThread ct: computingthreads){
			if(ct.getStatus()==Status.RUNNING) num++;
		}
		return num;
	}
	
} //end of class
