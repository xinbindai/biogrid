package org.xdai.biogrid;

public class ComputingThread implements Runnable {

	static String PROCESSORSNUMBER = "`cat /proc/cpuinfo|grep ^processor|wc -l`";
	
	//static int RUNNING = 1;
	//static int FINISHED = 0;
	//static int FAILED = -1;  //not required
	//static int READY = -2; // the NodeThread is ready to run command.
    
	private ComputingNode computingnode;
	private int cpunum;  // # of CPU core in this computing node(host)
	private String cmd;
	private int slicepos; // position of slice file on command, first parameter=1, programname=0;
    private int maxFailedTimes;

	
	private Slice curslice;
	private RunProcess rp; 

	private boolean active; // if a Nodethread is failed to run a command, active will be false.
	private int status;
	private long starttime;

	public ComputingThread(ComputingNode computingnode, int cpunum, String cmd, int slicepos, int maxFailedTimes) {
		this.computingnode = computingnode;
		this.cpunum = cpunum;
		this.cmd=cmd;
		this.slicepos=slicepos;
		this.maxFailedTimes=maxFailedTimes;
		
		this.active = true;
		this.status=Status.READY;
		this.starttime=0;
		this.curslice=null;
		this.rp=null;
	}

	public void setActive(boolean active) {
		if(this.active==active) return;
		System.err.println("The computing thread " + getDescStr() +" has been " + (active?"activated":"deactivated"));
		this.active=active;	
	}
	public boolean isActive() {		return active;	}

	
	public long getElapsedTimeInMilliSecs(){
	    if(status==Status.RUNNING) return System.currentTimeMillis()-starttime;
	    else                                return 0;
	}

	public void run() {
		try{
			runcmd();
		}catch(java.lang.Throwable e){
			int tmpid=-1;
			if(this.curslice!=null) tmpid=this.curslice.getId();
		    System.err.println( "\n********* Computing thread " + this.getDescStr()
		    		+ " failed while analyzing slice #" + tmpid 
		    		+ ", error message:");
		    e.printStackTrace(System.err);
	    	setActive(false);
	    	releaseSlice(Status.READY);
		}
	}

	public void runcmd() {
		// transfer slice-running file to remote machine
		if (!this.computingnode.isLocalhost()) {  //,"-o", "ConnectTimeout=3"
			String tmpcmd[] = { "scp", this.curslice.getSlicefn(),	computingnode.getHostname() + ":" + this.computingnode.getWorkdir() + "/" };
			//String tmpcmd[] = { "bash", "-c", "sleep 1;scp " + this.curslice.getSlicefn()+" "+computingnode.getHostname() + ":" + this.computingnode.getWorkdir() + "/" };
			//String tmpcmd[] = { "rsync", "-az", "-e", "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null", this.curslice.getSlicefn(),	computingnode.getHostname() + ":" + this.computingnode.getWorkdir() + "/" };
			//rsync -avz -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
			rp=new RunProcess(tmpcmd);
			ProcessRet ret = rp.getRet(3600000);
			if (ret.retcode!=Status.FINISHED) {
				System.err.println("\nFailed to transfer slice file for computing thread  " 
				                 + this.getDescStr() + ", error message:\n" 
			                     + ret.stderr
			                     + "Recycle the running slice #" + curslice.getId());
		    	setActive(false);
		    	releaseSlice(Status.READY);
				//if (computingnode.isLocalhost()) setActive(true);
	            System.err.println();			
				return;
			}
		}

		// update cmd
		String cmdlist[] = cmd.split("[ \t]+");
		cmdlist[this.slicepos] =  curslice.getSlicefn();
		
		String[] tmpcmd=new String[3];
		if (this.computingnode.isLocalhost()) {
			//tmpcmd[0] = "bash";
			//tmpcmd[1] = "-c";
			tmpcmd = cmdlist;
		}else{
		    String finalcmd = "";
		    for (int i = 0; i < cmdlist.length; i++) {
			    //if (cmdlist[i].equals("PROCESSORSNUMBER"))
    	 		//sfinalcmd = finalcmd + " " + ComputingThread.PROCESSORSNUMBER;
		    	//else
			    finalcmd = finalcmd + " " + cmdlist[i];
		    }
		    //if (!this.computingnode.isLocalhost()) {
			finalcmd = finalcmd + ";ret=$?;rm -f " + curslice.getSlicefn()	+ ";exit $ret";
		    //}
		    tmpcmd[0] = "ssh";
		    tmpcmd[1] = computingnode.getHostname();
		    tmpcmd[2] = finalcmd;
		}
		
		// then run command
		rp=new RunProcess(tmpcmd, curslice.getOutputfn(), curslice.getErrorfn());
		ProcessRet ret = rp.getRet();
		if(ret.retcode == Status.FINISHED) {
  		    curslice.getSm().updateAveragetime(System.currentTimeMillis()-starttime);
		    releaseSlice(Status.FINISHED);   //slice status is set to FINISHED
		}else if(ret.retcode==Status.FAILED) {  //subprocess failed because of process issue
			    System.err.println( "\n********* Failed to analyze slice #"+curslice.getId()+" with computing thread " 
			                  + this.getDescStr() + ", error message: ***********\n"
		                      + Tools.getFileContent(curslice.getErrorfn())
		                      );
			    curslice.setFailedTimes(curslice.getFailedTimes()+1);
			    if(maxFailedTimes>0&&curslice.getFailedTimes()>=maxFailedTimes) {
				    System.err.println("The slice #"+curslice.getId()+" has failed for " + curslice.getFailedTimes() + " time(s), skip it!!!");
				    System.err.println("Release computing thread " + this.getDescStr() + " for other tasks");
				    setActive(false);
				    releaseSlice(Status.FAILED);  //always failed, should be the problem of slice itself
			    }else{
			    	System.err.println("Recycle the slice #" + curslice.getId());
			    	System.err.println("Release computing thread " + this.getDescStr() + " for other tasks");
			    	setActive(false);
			    	releaseSlice(Status.READY);
			    }
		}else if(ret.retcode==Status.FAILEDTORUN) {  //subprocess failed to be invoked  
				//It is FAILEDTORUN invoked from internal error (failed to launch subprocess because of memory/resource issue)
				//the slice data is fine, always be ready for re-try
				System.err.println( "\n********* Failed to launch subprocess for slice #"+curslice.getId()+" with computing thread " 
		                  + this.getDescStr() + ", error message: ***********\n"
	                      + ret.stderr
	                      );
			    System.err.println("Recycle the slice #" + curslice.getId());
			    System.err.println("Release computing thread " + this.getDescStr() + " for other tasks");
				setActive(false);
				releaseSlice(Status.READY);
		}
	}

	public String getCmd() {	return cmd;	}

	public void setCmd(String cmd) {  this.cmd = cmd;	}

	public String getDescStr() {	return "Node#" + this.cpunum + ":" + this.computingnode.getHostname(); 	}

	public int getStatus() { return status;	}

	public void setStatus(int status) {	this.status = status; }

	public Slice getCurslice() { return curslice;	}

	public void bindSlice(Slice tmpslice) {
		tmpslice.setStatus(Status.RUNNING);
		this.curslice = tmpslice;
		this.starttime = System.currentTimeMillis();
		this.status = Status.RUNNING;  //computingthread are assigned sequentially in dispatch function, it is safe to lock it at last
	}

	public void releaseSlice(int sliceNewStatus) {
		this.curslice.setStatus(sliceNewStatus);  //must release slice firstly!!!!
		this.status=Status.READY;     //then it is ready for computingthread for other task
		//this.curslice = null;
	}
	
	public ComputingNode getComputingnode() {
		return computingnode;
	}

	public RunProcess getRp() {
		return rp;
	}

}
