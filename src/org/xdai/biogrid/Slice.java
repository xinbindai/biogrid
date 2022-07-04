package org.xdai.biogrid;

import java.io.File;

public class Slice {

	//public final static int READY=0;
	//public final static int RUNNING=1;
	//public final static int FINISHED=2;
	//public final static int FAILED=3;
	

    public final static String SLICE_FILE_SUFFIX = ".slice";
	public final static String OUTPUT_FILE_SUFFIX = ".out";
	public final static String ERROR_FILE_SUFFIX = ".err";
	
    private String slicefn;
    private String outputfn;
    private String errorfn;
    private int failedTimes;

	private int id;
    private int status;
    private long size;
    private int idForGroup;
    private SliceManager sm;
    
    public Slice(SliceManager sm, String workdir, String prefix, int id, int idForGroup) throws Exception{
    	this.sm=sm;
    	slicefn=Slice.makeSlicefn(workdir, prefix, id);
    	outputfn=slicefn+Slice.OUTPUT_FILE_SUFFIX;
    	errorfn=slicefn+Slice.ERROR_FILE_SUFFIX;
    	
    	this.failedTimes=0;
    	this.id=id;
    	this.status=Status.READY;
    	File f = new File(slicefn);
    	if(!f.exists()) throw new Exception("Slice file "+slicefn+" doesn't exist");
        this.size=f.length();
        this.idForGroup=idForGroup;
    }

    public static String makeSlicefn(String workdir, String prefix, int id){
    	return workdir+"/"+prefix+id+Slice.SLICE_FILE_SUFFIX;
    }

	public int getId() {
		return id;
	}

	public long getSize() {
		return size;
	}


	public int getStatus() {
		return status;
	}


	public void setStatus(int status) {
		//if(status!=Slice.READY&&status!=Slice.RUNNING&&status!=Slice.FINISHED) throw new Exception("Invalid slice status code ("+status+")");
		this.status = status;
	}


	public String getErrorfn() {
		return errorfn;
	}


	public String getOutputfn() {
		return outputfn;
	}


	public String getSlicefn() {
		//if(status!=Slice.READY) System.err.println("Warning: try to get Slice filename when its status is not READY. Slice ID: "+id);
		return slicefn;
	}

	public int getIdForGroup() {
		return idForGroup;
	}

	public void setIdForGroup(int idForGroup) {
		this.idForGroup = idForGroup;
	}

	public SliceManager getSm() {
		return sm;
	}

	public int getFailedTimes() {
		return failedTimes;
	}

	public void setFailedTimes(int failedTimes) {
		this.failedTimes = failedTimes;
	}
    
}
