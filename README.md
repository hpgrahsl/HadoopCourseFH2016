# HadoopCourseFH2016
Repository for my introductory course to bare-bones Hadoop MapReduce with Java. The course has been held for the last time this year. We'll switch to a different technology stack next summer.

### Exercises

Contains 4 different examples that are implemented by means of one or more Hadoop MapReduce Jobs. It's recommended to do them in order (1-4) as difficulty is increasing.

### StarterKitHadoop

Contains the Maven Project as well as all the input data that's needed for the exercises. Please be patient after importing the project to your IDE as there are quite a few dependencies that need to be resolved / downloaded by Maven before you are able to start.

*NOTE for Windows Users ONLY:*
Hadoop needs access to some native code (e.g. when performing file I/O). When you are working in a Windows development environment you need to do the following steps before you can start:

1. Set the PATH environment variable to include the hadoop/bin/ directory of your local project folder (see https://github.com/hpgrahsl/HadoopCourseFH2016/tree/master/StarterKitHadoop/hadoop/bin)

2. Also install the included Visual C++ Redistributable Package which can be found in the hadoop/ directory of your local project folder. It's the version that was used for the pre-compiled windows binaries.

Furthermore, make sure that you use 64-Bit only version of all included components (OS, JDK, IDE,...) otherwise you may get strange Exceptions due to Link Errors with the Native Code.

### SolutionKitHadoop

Contains the Maven Project for sample solutions to the 4 exercises. Of course there are numerous different ways to get things done. Feel free to implement your very own approach ;-)
 
