Hadoop.Client
=============

.NET client for Hadoop.

The goal of this lib is to provide simple and consistent way to interact with Hadoop cluster. 

It consists of two areas:
 - HDFS api which allows to connect to and play with Hadoop file system
 - Hadoop Job Management api for scheduling and controlling various hadoop jobs, including MapRecduce, Hive and Pig

This lib has been inspired by Microsoft Hadoop SDK. What differs the styles of both codebases is my motivation for simplifying things by getting rid of all of the unnecessary stuff and pesky abstractions. I've also removed all references to everything related to Azure. Enjoy. 

For sample usage check Hadoop.Client.Tests project
