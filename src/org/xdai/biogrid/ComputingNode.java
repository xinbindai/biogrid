package org.xdai.biogrid;

import java.util.ArrayList;
//import java.util.Arrays;

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

	private String username;  //ssh username only for remote node
	private String[] ssh_parameters;
	
    public ComputingNode(MasterNode masternode, String hostname, int totalNumberOfThreads, int groupID, String workdir,
	                      String username, String[] ssh_parameters){ //grouID is # of computing node in pool
      this.masternode=masternode;
      this.hostname=hostname;
      this.totalNumberOfThreads=totalNumberOfThreads;
      this.groupID=groupID;
      this.workdir=workdir;
      this.localhost=Tools.isLocalHost(hostname);

	  this.hostname = hostname;
	  this.username = username;
	  this.ssh_parameters = ssh_parameters;
      
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



	public static ArrayList<String> makeCmdHeader(ComputingNode cn){
		return Tools.makeCmdHeader(cn.isLocalhost(), cn.username, cn.hostname, cn.ssh_parameters);
	}


 	public boolean makeWorkdir(){

		ArrayList<String> tmpcmd = ComputingNode.makeCmdHeader(this);
		tmpcmd.add(makeworkdircmd);
		String[] cmd_array = new String[tmpcmd.size()];
		tmpcmd.toArray(cmd_array);

		ProcessRet ret = RunProcess.runcmd(cmd_array,3000);
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

		ArrayList<String> tmpcmd = ComputingNode.makeCmdHeader(this);
		tmpcmd.add(delworkdircmd);
		String[] cmd_array = new String[tmpcmd.size()];
		tmpcmd.toArray(cmd_array);
		
		RunProcess.runcmd(cmd_array);
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
