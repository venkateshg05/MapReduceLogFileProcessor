# MapReduceLogFileProcessor
by Venkatesh Gopalakrishnan
UIN: 655290219
NetID: vgopal9@uic.edu

Using Hadoop Map/Reduce concept, this program processes log files &amp; produce the required statistics 
AWS Deployment YouTube link: https://youtu.be/trSEGND79lE

Program design and models 
The entry point to the program is the MRDriver object. It sets up the 4 M/R processes designed for the 4 tasks. 
It runs the M/R process from the main function - runMRProcesses. The main funtion takes in 2 command line args. One, path to the input folder and two, path to the output folder. That's all is required to run the main program.

There's also a application.conf file that houses all the app parameters. Users can read the comments in the file for playing with the configs.

Each of the 4 tasks have 4 M/R programs dedicated to them. All 4 have been split into 4 scala Objects. Their functions are described in the comments in the respective files.

Additionally, utility functions are in the HelperUtils package. They take care of setting up the logger, getting the parameters from the config files, matching the regex pattern in log messages & calculating the time interval using binary search.

How program is assembled?
The input folder is loaded with a single log file or a large log file split into multiple small shards. To split a large file into shards, user can use Git Bash to split the files. Using the command split <FILE> -l <NUM_OF_LINES>, user can get the desired shards. Once the input folder is loaded & the application.conf file is setup with the required parameters, the user needs to run the main function in the MRDriver object (runMRProcess). The program should terminate after producing 4 output subfolders in the specified output directory.

The final jar file can be produced by running the sbt assemble command. 

How all components of mappers and reducers are deployed?
The jar file & the input file(s) are uploaded to the aws s3 bucket. The EMR cluster is created & the steps defined to use a custom jar file. The path to the jar file in s3 is shared. The input and output paths are passed as args. Clicking run should run the main function. The outputs should be available in the output path specified.
