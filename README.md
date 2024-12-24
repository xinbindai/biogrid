# About BioGrid

Biogrid was designed to perform perallel computing in a cluster of Linux Hosts. The master node is the Linux host launching the application. Java-based tool that can split a large task into multiple slices and distribute into Linux servers (computing nodes) through SSH.

# Usage Example

'''
java -jar biogrid.jar -s /tmp/biogrid-test -p 1 -m 2 -c line -r 0.5 -f 99999 -P "-p 22 -i /home/user/id_rsa" -n node.ini "/tmp/biogrid-test/test.sh /tmp/biogrid-test/input.txt 10"
'''

The above biogrid command will let master node do these steps:

1. Parse `node.ini` (from `-n`) to get a list of computing nodes and preapre to use `-p 22 -i /home/xdai/id_rsa` (specifed by `-P`) as parameter to access these computing nodes by SSH.
1. Take `/tmp/biogrid-test/test.sh /tmp/biogrid-test/input.txt 10` as template of command line to run in computingn nodes.
1. Since the command file (`/tmp/biogrid-test/test.sh`) is located in `/tmp/biogrid-test/` which is only available in master node, we use `-s /tmp/biogrid-test` to copy the folder into computing nodes.
1. Before calling the above command line in computing nodes by SSH, take the 1st (`-p 1`) parameter (`/tmp/biogrid-test/input.txt`) and split the file by line (`-c line`). Try to cut the file into as many as possible lines (`-f 99999`) but make sure each of the slice file contians two lines (`-m 2`) at least.
1. After slice files are ready, Biogrid master node do the following 4 steps in parallel:
1. Send one of generated slice file to a computing node.
1. Replace `/tmp/biogrid-test/input.txt` with the slice file and call the updated command line in the computing node by SSH.
1. Monitor the SSH command until the process completes. stdout and stderr will be collected and deposited in master node tmp working folder.
1. If a computing task fails, master node can resubmit the failed task.
1. After all slices are processed by computing nodes, master node will merge all stdout and stderr into two files and exit.

# Parametrs: 

- -s: folders in mater node which will be sychronized to all computing nodes before parallel computing start. Usually, the folders include program, library and related data that will be required for calculation in computing nodes. Multiple directories are separated by ':'
- -p: The index position of file in command line string. The file will be splitted into slices
- -m: slice min size
- -c: The way to split file, only value is `line`. The file will be by lines guided by `-m` and `-f`.
- -r: A factor that is used to glaobally adjust number of cores to be used in each comuting node
- -f: factor of slice. Larger value will split file into smaller size until it reach `-m`.
- -P: Paramter for SSH client
- -n: node confile file which contains a list of computing node with number of CPU cores
- last parameters is a command line string which will be called in each computing nodes

