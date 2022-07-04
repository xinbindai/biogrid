package org.xdai.biogrid;

import java.util.ArrayList;

public class ParallelRunProcess {

	private ArrayList<RunProcess> arp;
	
	public ParallelRunProcess(){
		arp = new ArrayList<RunProcess>();
	}
	
	public void add(String[] cmd){
		arp.add(new RunProcess(cmd));
	}
	
	public void add(RunProcess rp){
		arp.add(rp);
	}
	
	public int getSize(){
		return arp.size();
	}

	public void waitFor(){
		while (true) {
			boolean hasRunning=false;
			for (int i = 0; i < arp.size(); i++) {
				if (arp.get(i).getStatus() == Status.RUNNING)  hasRunning = true;
			}
			if(!hasRunning) break;
			try { Thread.sleep(20); } catch (InterruptedException e) {	}
		}
	} //end of function
	
	public ArrayList<RunProcess> getFinishedRunProcesses(){
		waitFor();
		ArrayList<RunProcess> finishedarp = new ArrayList<RunProcess>();
		for (int i = 0; i < arp.size(); i++) {
			if (arp.get(i).getStatus() == Status.FINISHED)  finishedarp.add(arp.get(i));
		}
		return finishedarp;
	}
	
	public ArrayList<RunProcess> getFailedRunProcesses(){
		waitFor();
		ArrayList<RunProcess> failedarp = new ArrayList<RunProcess>();
		for (int i = 0; i < arp.size(); i++) {
			if (arp.get(i).getStatus() == Status.FAILED 
					|| arp.get(i).getStatus() == Status.FAILEDTORUN)  failedarp.add(arp.get(i));
		}
		return failedarp;
	}


}
