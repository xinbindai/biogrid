package org.xdai.biogrid;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class RunProcess {

	public static int STDOUT = 1;
	public static int STDERR = 2;

	//public static int RUNNING = 1;
	//public static int FINISHED = 0;
	//public static int FAILED = -1;
	//public static int READY = -2; // ready to run command.
	//public static int FAILEDTORUN = -3;  //failed to launch the process, IOexpception, outofmemory etc
	
	// inner class, save STDOUT/STDERR message------------
	private class RecordResult implements Runnable {
		private RunProcess rp;
		private int out_err;

		InputStream in = null;
		OutputStream out = null;
		
		public RecordResult(RunProcess rp, int myout_err) {
			this.rp = rp;
			out_err = myout_err; // 1=stdout, 2=stderr
		}

		public void close(){
			try {
				in.close();
			} catch (Exception e) {
					//e.printStackTrace();
			}

			try {
				if (out_err == RunProcess.STDOUT) 
					rp.process.getOutputStream().close();
				if (out_err == RunProcess.STDERR) 
					rp.process.getErrorStream().close();
			} catch (Exception e) {
					//e.printStackTrace();
			}

			try {
				out.close();
			} catch (Exception e) {
					//e.printStackTrace();
			}
			
			if (out_err == RunProcess.STDOUT) {
				try{
				    if(rp.outfn==null&&out!=null) ret.stdout += out.toString();    //ret[1] = output of STDOUT
			    }catch(Exception e){}
				rp.isOutputReady = true;
			}else{
				try{
				    if(rp.outfn==null&&out!=null) ret.stderr += out.toString();
			    }catch(Exception e){}
				rp.isErroutReady = true;
			}
		}
		
		public void run() {
			try {
				if (out_err == RunProcess.STDOUT) in = rp.process.getInputStream();
				else 					          in = rp.process.getErrorStream();

				if(rp.outfn==null) {  //if non output/err filename, save output/err into an ArrayStream
					out = new ByteArrayOutputStream(1024*256);
				}else{
					if (out_err == RunProcess.STDOUT) out = new FileOutputStream(rp.outfn);
					else 					          out = new FileOutputStream(rp.errfn);
				}
				
				byte[] b = new byte[1024*128];
				for (;;) {
					int len = in.read(b);
					if (len == -1)	break;
					out.write(b, 0, len);
				}
			} catch (IOException e) {
				rp.hasExceptionWhenAcceptOutErr = true;
			}finally{
			    close();
			}
		} // end of function
	} // end of inner class definition

	private String[] cmd;
	private String outfn=null;
	private String errfn=null;

	private Process process;
	private ProcessRet ret;
	private Object relobj;
    private String cmdstr;
    private String message;
	
	private boolean isOutputReady = true;
	private boolean isErroutReady = true;
	private boolean hasExceptionWhenAcceptOutErr = false;

	private RecordResult rr1=null,rr2=null;
    
    public RunProcess(String[] cmd) {
    	init0(cmd,null,null);
    }

    public RunProcess(ArrayList<String> cmdlist) {
		String[] cmd = new String[cmdlist.size()];
		cmdlist.toArray(cmd);
    	init0(cmd,null,null);
    }


	public RunProcess(String[] cmd, String outfn, String errfn) {
		init0(cmd,outfn,errfn);
	}

    public RunProcess(ArrayList<String> cmdlist, String outfn, String errfn) {
		String[] cmd = new String[cmdlist.size()];
		cmdlist.toArray(cmd);
    	init0(cmd, outfn, errfn);
    }


	private void init0(String[] cmd, String outfn, String errfn){
		try{
			init(cmd,outfn,errfn);
		}catch(java.lang.Throwable e){
            if(e instanceof java.lang.OutOfMemoryError){
           	    //resource restriction, wait 1000 msec.
        	    try{Thread.sleep(1000);}
        	    catch(Exception e2){}
            }
            if(process!=null) process.destroy();
            if(rr1!=null) rr1.close();
            if(rr2!=null) rr2.close();
			ret.retcode = Status.FAILEDTORUN;
			ret.stderr += e.getMessage();  //failed to launch subprocess. so there is no stderr file to save error message
			process = null;
		}
	}
	
	private void init(String[] cmd, String outfn, String errfn) throws IOException{
		this.cmd=cmd;
		this.outfn=outfn;
		this.errfn=errfn;
		
		process=null;
		ret = new ProcessRet();
		relobj=null;
		cmdstr="";
		message="";
		
		if(this.cmd!=null&&this.cmd.length>0){
			for(String tmp:this.cmd) cmdstr=cmdstr+" "+tmp;
		}
		process = Runtime.getRuntime().exec(this.cmd);

		this.isErroutReady = false;
		this.isOutputReady = false;

		rr1=new RecordResult(this, RunProcess.STDOUT);
		Thread t1 = new Thread(rr1);
		t1.start();
		
		rr2=new RecordResult(this, RunProcess.STDERR);
		Thread t2 = new Thread(rr2);
		t2.start();
	}

	public ProcessRet retrieveRet(){   //directly get ret, useful to call after calling getRet()
		return ret;
	}
	
	//if terminated in specified time
    private boolean waitforinmsec(long maxtimeInmilliseconds){
		long starttime=System.currentTimeMillis();
        boolean terminated=true;  //assume it is terminated
		while(true) {
            try{
                process.exitValue();
            }catch(IllegalThreadStateException e){
            	terminated=false;      //throw exception means it is not yet terminated
            }
            if(terminated) break;
		    long tmpelapsedtime=System.currentTimeMillis()-starttime;
            if(tmpelapsedtime>maxtimeInmilliseconds) break;
            
		    try{ Thread.sleep(1); }catch(Exception e){}
		}
		return terminated;
    }
	
	public ProcessRet getRet(long maxtimeInmilliseconds) {
		if (ret.retcode == Status.FAILEDTORUN)	return ret;
		
		if(maxtimeInmilliseconds<=0) {
		    try {
 		        process.waitFor();
			    while (!this.isErroutReady || !this.isOutputReady) {
				    Thread.sleep(100);
			    }				
		    } catch (InterruptedException e) {
			    //e.printStackTrace();
		    }
		}else{
  		    try {

  		    	long starttime=System.currentTimeMillis();

  		    	while (!this.isErroutReady || !this.isOutputReady) {
				    long tmpelapsedtime=System.currentTimeMillis()-starttime;
	                if(tmpelapsedtime>maxtimeInmilliseconds){
                        //this.hasExceptionWhenAcceptOutErr=true;    //IO error, failed to close IO stream from subprocess in time
                        try { process.destroy(); } catch(Exception e) {}
                        this.rr1.close();
                        this.rr2.close();
                        message = message+"The process '"+cmdstr+"' has been killed because its running time exceeded uplimit ("+maxtimeInmilliseconds+" milliseconds)\n";
                        break;
	                }
				    Thread.sleep(1);
				}

  		    	if(!waitforinmsec(3000)){  //if process is not terminated in 3000 milliseconds
  	                try { process.destroy(); } catch(Exception e) {}
  	                message = message+"The process '"+cmdstr+"' has been killed because it failed to exit in time\n";
  		    	}  		    	
			    
				
		    } catch (InterruptedException e) {
			    //e.printStackTrace();
		    }
		}
		
		if (hasExceptionWhenAcceptOutErr){
			ret.retcode = Status.FAILED;
			message=message+"An IO exception occurred while reading STDOUT/STDERR of process.\n";
		}else{
            try{
			    if(process.exitValue()==0)  ret.retcode=Status.FINISHED;  
			    else                        ret.retcode=Status.FAILED;
            }catch(IllegalThreadStateException e){  //still not terminated, try to kill again
            	ret.retcode=Status.FAILED;
            	message=message+e.getMessage()+"\n";
            	try { process.destroy(); } catch(Exception e2) {}
			}
		}
		
		if(ret.retcode==Status.FAILED&&!message.equals("")){
		  if(outfn==null){
			  ret.stderr=ret.stderr+message;
		  }else{
			  try{ 
				 OutputStream tmpout = new FileOutputStream(errfn);
			     tmpout.write(message.getBytes());
			     tmpout.close();
			  }catch(Exception e){}
		  }
		}
		process=null;
		return ret;
	}

	public ProcessRet getRet() {
		return getRet(-1);
	}
	
	public int getStatus() {
		int exitValue = 0;
		if (process == null) return Status.READY;
		try {
			exitValue = process.exitValue();
		} catch (java.lang.IllegalThreadStateException e) {
			return Status.RUNNING;
		}
		if (exitValue == 0)  return Status.FINISHED;
		else    			 return Status.FAILED;
	}

	static ProcessRet runcmd(String[] tmpcmd) {
		RunProcess rp = new RunProcess(tmpcmd);
		return rp.getRet();
	}

	static ProcessRet runcmd(String[] tmpcmd, long maxtimeInmilliseconds) {
		RunProcess rp = new RunProcess(tmpcmd);
		return rp.getRet(maxtimeInmilliseconds);
	}

	public Object getRelobj() {
		return relobj;
	}

	public void setRelobj(Object relobj) {
		this.relobj = relobj;
	}

	public Process getProcess() {
		return process;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
