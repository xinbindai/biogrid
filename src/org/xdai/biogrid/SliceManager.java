package org.xdai.biogrid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Scanner;

public class SliceManager {

	private String prefix;  //general prefix of slice file name
	private String workdir;
	private String inputfn; // input file name
	private int sliceTotalNumber; // total number of slices
	private int slicesize;
	private int chopmode;
	private int minslicesize;
	
	private String finalResultOutputFile;
	private String finalResultErrFile;
    private boolean groupmode;
    private String slicesort;
	
	public final static int FASTA = 0;
	public final static int LINE = 1;

	private Slice[] slicelist = null;  //slice list that is sorted by descendent file size
	private Slice[] slicelist2 = null;  //slice list by slice ID asc
	
    private long totaltime=0;
    private int finishedslicesnum=0;
    private long averagetime=0;
    private double progress=0;
    
    public synchronized void updateAveragetime(long runningtime){
    	totaltime+=runningtime;
    	finishedslicesnum++;
    	averagetime=totaltime/finishedslicesnum;
    	progress=1.0*finishedslicesnum/sliceTotalNumber;
    }  
	
	private class Slicecomparator implements java.util.Comparator<Slice>{
		@Override
		public int compare(Slice o1, Slice o2) {
			return (int)(o2.getSize()-o1.getSize());
		}
		
	}
	
	private void initSlicelist() throws Exception{
		this.sliceTotalNumber=SliceManager.slicing(inputfn, workdir, prefix, slicesize, chopmode, minslicesize);
		if(this.sliceTotalNumber<1) throw new Exception("There is no slice for parallel computing analysis. Check your input file.");
		slicelist=new Slice[this.sliceTotalNumber];  
		for(int i=0;i<this.sliceTotalNumber;i++){
			Slice tmp=new Slice(this, workdir, prefix, i, i);
			if(groupmode){
 			   Scanner sc = new Scanner(tmp.getSlicefn());
			   String line1 = sc.nextLine();
			   sc.close();
		       Matcher m = patGroup.matcher(line1);
               if(m.find()) tmp.setIdForGroup(Integer.valueOf(m.group(1)));
			}
			slicelist[i]=tmp;
		}
		slicelist2= new Slice[slicelist.length];
		for(int i=0;i<slicelist2.length;i++){
		   slicelist2[i]=slicelist[i];; 
		}
		if(this.slicesort.equals("sr")) {
            java.util.Arrays.sort(slicelist, new Slicecomparator());  //larger slice will be firstly analyzed.
		}
/*
        for(int i=0;i<this.sliceTotalNumber;i++){
        	Slice tmp = slicelist2[i];
        	System.err.println(getSliceFileName((int)(tmp[0]))+"\t"+tmp[1]+"\t"+tmp[2]);
        }
*/	
	}

	
	public static SliceManager SliceManagerFactory(String uniqueprefix,	int numComputingThreads, String inputfn, int slicefactor,
			           int minslicesize, String tmpworkdir, int chopmode, boolean groupmode, String slicesort) throws Exception {
		int totalseq = 0;
		if (chopmode == FASTA)
			totalseq = getSeqNumberFromFasta(inputfn);
		else if (chopmode == LINE)
			totalseq = getValidLineNumber(inputfn);
		if (totalseq == 0) throw new Exception("No sequences in input file.");
		int slicesize = (int) ((totalseq - 1) / (numComputingThreads * slicefactor)) + 1;
		if (slicesize > minslicesize)
			slicesize = slicesize / minslicesize * minslicesize;
		else
			slicesize = minslicesize;
		SliceManager sm = new SliceManager(uniqueprefix, inputfn, tmpworkdir, slicesize, chopmode, groupmode, slicesort, minslicesize);
		return sm;
	}

	public SliceManager(String uniqueprefix, String inputfn, String workdir, int slicesize, int chopmode, boolean groupmode, String slicesort, int minslicesize) throws Exception {
		this.prefix = uniqueprefix;
		this.inputfn = inputfn;  //input file name
    	this.workdir = workdir;
    	this.slicesize = slicesize;
    	this.chopmode = chopmode;
		this.groupmode = groupmode;
		this.slicesort = slicesort;
		this.minslicesize = minslicesize;

		this.finalResultOutputFile = inputfn + Slice.OUTPUT_FILE_SUFFIX;
		this.finalResultErrFile = inputfn + Slice.ERROR_FILE_SUFFIX;
		
		initSlicelist();
	}

	public void MergeOutputFile(String outputfile, String seperate) throws IOException {
		if (outputfile != null && !outputfile.equals("")) {
			finalResultOutputFile = outputfile;
			finalResultErrFile = outputfile + Slice.ERROR_FILE_SUFFIX;
		}
		OutputStream out1 = new FileOutputStream(finalResultOutputFile);
		OutputStream out2 = new FileOutputStream(finalResultErrFile);
		MergeOutputFile(out1, out2, seperate);
	}

	public void MergeOutputFile(OutputStream out, OutputStream err,	String seperate) throws IOException {
		OutputStream out1 = out;
		OutputStream out2 = err;

		ArrayList<Slice> als = new ArrayList<Slice>(); 
		byte[] sep = null;
		if (seperate != null) sep = seperate.getBytes();
		for (int i = 0; i < slicelist2.length; i++) {
            //merge error
			InputStream in2 = new FileInputStream(new File(slicelist2[i].getErrorfn()));
			if (sep != null) out2.write(sep);
			byte[] b2 = new byte[65536];
			for (;;) {
				int len = in2.read(b2);
				if (len == -1) break;
				out2.write(b2, 0, len);
			}
			in2.close();
			
			//merge output
			if(slicelist2[i].getStatus()==Status.FAILED){
				als.add(slicelist2[i]);
				continue;
			}
			InputStream in1 = new FileInputStream(new File(slicelist2[i].getOutputfn()));
			if (sep != null)	out1.write(sep);
			byte[] b1 = new byte[65536];
			for (;;) {
				int len = in1.read(b1);
				if (len == -1) break;
				out1.write(b1, 0, len);
			}
			in1.close();

		}
		out1.close();
		out2.close();
		
		if(als.size()>0){
			System.err.print("These slices (IDs) failed, their output are excluded from output file. You may check error file for more information: ");			
			for(Slice tmp:als){		System.err.print(tmp.getId()+" ");	}
			System.err.println();
		}
		
	} // end of function

	static Pattern patGroup=Pattern.compile("^[#>]groupID=([0-9]+)");    //id start from 0 to slicetotalnum-1 
	
	//get a slice whose status is READY and match group ID
	public Slice getReadySlice(int groupIDOfComputingNode, int totalNumberOfComputingNodes) { // prefer to choose the biggest slice from specified group(node computer)
        Slice readyslice=null;
		for(int i=0;i<slicelist.length;i++){
        	if(slicelist[i].getStatus()!=Status.READY) continue;
		    if(slicelist[i].getIdForGroup()%totalNumberOfComputingNodes!=groupIDOfComputingNode) continue;
		    readyslice=slicelist[i];
		    if(readyslice!=null) break;
        }
		return readyslice;
	}

	//get a slice whose status is READY and match group ID
	public Slice getReadySlice() {
        Slice readyslice=null;
		for(int i=0;i<slicelist.length;i++){
        	if(slicelist[i].getStatus()==Status.READY) {
        		readyslice=slicelist[i];
        		break;
        	} 
        }
		return readyslice;
	}

	public int getSliceCount(int status){
        int count=0;
		for(int i=0;i<slicelist.length;i++){
        	if(slicelist[i].getStatus()==status) count++;
        }
		return count;
	}
	
	//do we still have any slice whose status is READY
	public boolean hasReadySlice() {
		boolean isAvailable=false;
		for(int i=0;i<slicelist.length;i++){
        	if(slicelist[i].getStatus()==Status.READY) {
        		isAvailable=true;
        		break;
        	} 
        }
		return isAvailable;
	}

	public boolean hasRunningSlice() {
		boolean isAvailable=false;
		for(int i=0;i<slicelist.length;i++){
        	if(slicelist[i].getStatus()==Status.RUNNING) {
        		isAvailable=true;
        		break;
        	} 
        }
		return isAvailable;
	}
	
	public boolean hasFailedSlice() {
		boolean isAvailable=false;
		for(int i=0;i<slicelist.length;i++){
        	if(slicelist[i].getStatus()==Status.FAILED) {
        		isAvailable=true;
        		break;
        	} 
        }
		return isAvailable;
	}


	//return the total number of slices
	public static int slicing(String inputfilename, String workdir, String prefix, int slicesize, int chopmode, int minslicesize) throws Exception {

		File fworkdir = new File(workdir);
		if (fworkdir.exists() && !fworkdir.isDirectory()) fworkdir.delete();
		if (!fworkdir.exists()) fworkdir.mkdirs();

		BufferedReader in = new BufferedReader(new FileReader(inputfilename));
		BufferedWriter out = null;
		int cursliceid = 0;
        int curseqnumber = 0;
		int lastfileslicesize=0;
        Pattern pempty = Pattern.compile("^\\s*$");
		
		while (true) {
			String seq = null;
			if(chopmode == FASTA) { 
			   seq = SliceManager.readSeq(in);
			}else if(chopmode == LINE){
			   seq = in.readLine();
			   if(seq!=null&&pempty.matcher(seq).matches()) continue;
			}
			if(seq==null) break;
			if(curseqnumber%slicesize==0){
				if (out != null) out.close();
				cursliceid=curseqnumber/slicesize;
				out = new BufferedWriter(new FileWriter(Slice.makeSlicefn(workdir, prefix, cursliceid)));
		        lastfileslicesize=0;
			}
			if(chopmode == LINE) seq=seq+"\n";
			out.write(seq);
		    lastfileslicesize++;
			curseqnumber++;
		}
		if (out != null) out.close();
		in.close();
        //the folllowing enhancement make sure any slice is larger than min slize size
		if(cursliceid>=1&&lastfileslicesize<minslicesize){
			System.out.println("Merge the last two slices to comply minslicesize");
			String lastslicefn = Slice.makeSlicefn(workdir, prefix, cursliceid);
			BufferedWriter sliceout =  new BufferedWriter(new FileWriter(Slice.makeSlicefn(workdir, prefix, cursliceid-1), true));
			BufferedReader slicein = new BufferedReader(new FileReader(lastslicefn));
			while(true){
                String tmp = slicein.readLine();
				if(tmp == null) break;
				sliceout.write(tmp+"\n");
			}
			if(sliceout!=null) sliceout.close();
			slicein.close();
			boolean res = new File(lastslicefn).delete();
			if(!res) throw new Exception("Failed to delete "+lastslicefn);
			cursliceid--;
		}

		return cursliceid+1;
	}

	public static String readSeq(BufferedReader in)  throws Exception  {
		  StringBuffer sb= null;
		  int marksize=1024*64;
		  
		  for(;;) {
			  in.mark(marksize);
	    	  String line = in.readLine();
              if(line==null) break;
              
	          if(line.startsWith(">") || line.startsWith("#")) {
	        	 if(line.length()>=marksize)  throw new Exception("FASTA header is too long, uplimit is "+marksize);
	   		     if(sb==null) {
	   		    	 sb=new StringBuffer();
	   		    	 sb.append(line);
	   		    	 sb.append("\n");
	   		     } else  {
	   		    	 in.reset();
	   		    	 return sb.toString();
	   		     }
	          }else if(sb!=null){
	        	  sb.append(line);
	        	  sb.append("\n");
	          }
	      }
		  if(sb==null) return null;
		  else         return sb.toString();
    }


	public String getFilename() {
		return inputfn;
	}

	public String  getFinalResultOutputFile() {
		return finalResultOutputFile;
	}

	public String getFinalResultErrFile() {
		return finalResultErrFile;
	}

	public int getSliceTotalNumber() {
		return sliceTotalNumber;
	}


	public synchronized long getAveragetime() {
		return averagetime;
	}


	public synchronized int getFinishedslicesnum() {
		return finishedslicesnum;
	}


	public  boolean isGroupmode() {
		return groupmode;
	}


	public synchronized double getProgress() {
		return progress;
	}


	public static int getSeqNumberFromFasta(String filename) throws Exception {
	    int number = 0;
		try{
		  BufferedReader in = new BufferedReader(new FileReader(filename));
		  while (true) {
			 String str = in.readLine();
			 if (str == null)
				break;
			 if (str.startsWith(">") || str.startsWith("#"))
				number++;
	
		  }
		  in.close();
		}catch(Exception e){ 
			if(e.getClass().getName().equals("java.io.FileNotFoundException")) 
				throw new java.io.FileNotFoundException("Error: Failed to find file '"+filename+"'");
			else 
				throw e; 
	    }
		return number;
	}


	public static int getValidLineNumber(String filename) throws Exception {
		int number = 0;
		try{
		   Pattern pempty = Pattern.compile("^\\s*$");
		   BufferedReader in = new BufferedReader(new FileReader(filename));
		   while (true) {
			 String str = in.readLine();
			 if (str == null) break;
			 if (pempty.matcher(str).matches())	continue;
			 number++;
		   }
		   in.close();
		}catch(Exception e){ 
			if(e.getClass().getName().equals("java.io.FileNotFoundException")) 
				throw new java.io.FileNotFoundException("Error: Failed to find file '"+filename+"'");
			else 
				throw e; 
	    }
		return number;
	}


	public int getSlicesize() {
		return slicesize;
	}
}
