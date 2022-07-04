package org.xdai.biogrid;

import java.util.ArrayList;

public class ComputingNode {
	private MasterNode masternode;
	private String hostname;
	private boolean localhost;
	private int totalNumberOfThreads;
	private int groupID;
	private String workdir;
	
	boolean disable;
	private ArrayList<ComputingThread> threads;
	private String makeworkdircmd;
	private String delworkdircmd;
	
    public ComputingNode(MasterNode masternode, String hostname, int totalNumberOfThreads, int groupID, String workdir){ //grouID is # of computing node in pool
      this.masternode=masternode;
      this.hostname=hostname;
      this.totalNumberOfThreads=totalNumberOfThreads;
      this.groupID=groupID;
      this.workdir=workdir;
      this.localhost=Tools.isLocalHost(hostname);
      
      this.disable=false;
      this.threads=new ArrayList<ComputingThread>();
  	  this.makeworkdircmd = "tmpname="	+ this.workdir + ";if [[ -e $tmpname && ! -d $tmpname ]]; then rm -f $tmpname;fi;"
                           +"if [[ ! -e $tmpname ]]; then mkdir -p $tmpname;chmod 1777 `dirname $tmpname`;fi;"
  			               +"if [[ -e $tmpname && -d $tmpname ]]; then echo \"exists\";fi";
      this.delworkdircmd = "tmpname=" + this.workdir + ";if [[ -e $tmpname && -d $tmpname ]]; then rm -rf $tmpname;fi";
    }

	public String getHostname() {
		return hostname;
	}

	public boolean isLocalhost() {
		return localhost;
	}

	public int getTotalNumberOfThreads() {
		return totalNumberOfThreads;
	}

	public ArrayList<ComputingThread> getThreads() {
		return threads;
	}
    
 	public boolean makeWorkdir(){
		String tmpcmd[] = { "ssh", hostname, makeworkdircmd };
		if (isLocalhost()) {
			tmpcmd[0] = "bash";
			tmpcmd[1] = "-c";
		}
		ProcessRet ret = RunProcess.runcmd(tmpcmd,3000);
		if(ret.retcode!=Status.FINISHED) {
			System.err.println();
			System.err.println("Failed to create tmpworkdir in computing host (" + hostname + "), disable it! Below is error message: ");
			System.err.println(ret.stderr);
			System.err.flush();
			disable=true;
			return false;
		}
		return true;
	}

	public void deleteWorkdir() {
		String tmpcmd[] = { "ssh", hostname, delworkdircmd };
		if (this.isLocalhost()) {
			tmpcmd[0] = "bash";
			tmpcmd[1] = "-c";
		}
		RunProcess.runcmd(tmpcmd);
	}

	public boolean isDisable() {
		return disable;
	}

	public String getWorkdir() {
		return workdir;
	}

	public int getGroupID() {
		return groupID;
	}
} //end of class
