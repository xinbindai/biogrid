package org.xdai.biogrid;

public class Status {
	public static final int RUNNING = 1;  //process is running or slice is being analyzed
	public static final int FINISHED = 0; //successfully finished/done
	public static final int FAILED = -1;  //failed after runnning
	public static final int READY = -2; // ready to run command or ready to called for analysis
	public static final int FAILEDTORUN = -3;  //failed to launch the process, IOexpception, outofmemory etc
}
