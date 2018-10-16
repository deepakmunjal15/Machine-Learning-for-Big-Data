from __future__ import print_function
import os
import sys

from pyspark import SparkContext
from ex1_classes import LogEntry


def parseFailedPasswordEntry(line):
    """
    This function will parse a line from the input file and return a LogEntry object that represents that line;
    Sample line: 'Apr  5 06:54:32 (none) sshd[12074]: Failed password for root from 1.30.20.148 port 7691 ssh2'
    """
    w = line
    w = w.split("Failed password for ")[1].split(" from ")
    """
    content of 'w' is ['root', '1.30.20.148 port 7691 ssh2']
    """
    username = w[0]
    if "invalid user " in username:
        # Clear the user name if it contains 'invalid user' string
        username = username.split("invalid user ")[1]
    w = w[1].split(" port ")
    """
    content of 'w' is ['1.30.20.148', '7691 ssh2']
    """
    remoteIp = w[0]
    w = w[1].split(' ')
    """
    content of 'w' is ['7691', 'ssh2']
    """
    remotePort = w[0]
    version = w[1]
    return LogEntry(username, remoteIp, remotePort, version, line)


def isFailedPasswordEntry(line):
    return "Failed password for " in line

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Missing parameters\n"\
              "Usage: ex1.py <inputFile> <outputFolder>", file = sys.stderr)
        exit(-1)

    filename = sys.argv[1]
    if not os.path.isfile(filename):
        print("Can not find input file : '%s'"%filename, file = sys.stderr)
        exit(-1);

    output_folder = sys.argv[2]
    if not os.path.isdir(output_folder):
        print("Can not find output directory: '%s'" % output_folder, file=sys.stderr)
        exit(-1);

    sc = SparkContext("local", "Exercise - 1: Log Parser App")

    """
    Create the initial RDD by opening the file: 02-auth.log
    """
    authLog = sc.textFile(filename)

    """
    Prepare the base RDD: logEntries
    --------------------------------
    1) Filter the "failed password attempts" in the log file.
        If a line contains "Failed password for " string then we will keep it otherwise it will be filtered out.
    2) Parse each line into a LogEntry object.
        Please check parseFailedPasswordEntry function for details.
        (Note: You don't have to use custom classes to represent your data, it might affect the performance but it will
        increase the readability of your code. In this example you can just use tuples instead of Logentry class.)
    3) Cache the RDD
        This step is important from the performance perspective. We will generate 3 different output by using this RDD
        as the base RDD.
    """
    logEntries = authLog.filter(isFailedPasswordEntry)\
                        .map(parseFailedPasswordEntry)\
                        .cache()

    """

    ## Sample - 01 ## Basic word count example for username

    Calculate username frequency: sortedUsernameFrequency
    -------------------------------------------------------
    1) map each LogEntry into a tuple of (username, 1)
    2) by calling reduceByKey we will merge entries by key, the lambda function we provided describes
        how the values need to be merged. In this case, we will simply add the values with each other.
        e.g assume that we have following entries
        ('root', 2), ('root', 5)
        applying reduceByKey with given lambda function will merge those entries into
        ('root', 7)
    3) sort the tuples by frequencies.
        after the reduceByKey transformation we have an unsorted entries in form of (username, frequency)
        lambda function provided to sortBy transformation will indicate which field should be used for sorting
    """
    sortedUsernameFrequency = logEntries\
                                .map(lambda entry: (entry.username, 1))\
                                .reduceByKey(lambda frequency1, frequency2: frequency1 + frequency2)\
                                .sortBy((lambda (username, frequency): frequency), ascending=False)

    """
    Save sortedUsernameFrequency to disk
    --------------------------------------
    1) Save to disk by calling saveAsTextFile
        use os.path.join to concat paths: output_folder + filename
    """
    sortedUsernameFrequency.saveAsTextFile(os.path.join(output_folder, "s01-sortedUsernameFrequency"))

    """

    ## Sample - 02 ## Basic word count example for remoteIP

    Calculate remoteIP frequency: sortedRemoteIPFrequency
    -------------------------------------------------------
    1) map each LogEntry into a tuple of (remoteIP, 1)
    2) by calling reduceByKey we will merge entries by key, the lambda function we provided describes
        how the values need to be merged. In this case, we will simply add the values with each other.
        e.g assume that we have following entries
        ('192.168.1.1', 2), ('192.168.1.1', 5)
        applying reduceByKey with given lambda function will merge those entries into
        ('192.168.1.1', 7)
    3) sort the tuples by frequencies.
        after the reduceByKey transformation we have an unsorted entries in form of (remoteIP, frequency)
        lambda function provided to sortBy transformation will indicate which field should be used for sorting
    """
    sortedRemoteIPFrequency = logEntries.map(lambda entry: (entry.remoteIp, 1))\
                                .reduceByKey(lambda frequency1, frequency2: frequency1 + frequency2)\
                                .sortBy((lambda (remoteIP, frequency): frequency), ascending=False)

    """
    Save sortedRemoteIPFrequency to disk
    --------------------------------------
    1) Save to disk by calling saveAsTextFile
        use os.path.join to concat paths: output_folder + filename
    """
    sortedRemoteIPFrequency.saveAsTextFile(os.path.join(output_folder, "s02-sortedRemoteIPFrequency"))

    """

    ## Sample - 03 ## Simple Key, Complex Value

    So far we calculated basic frequencies of username and remoteIP values. But we have no idea about the relation
    between the remoteIP and username variables.

    Calculate the remoteIP frequency & remoteIp <-> username mapping: sortedRemoteIpUsernameMapping
    ----------------------------------------------------------------------------------
    1) map each LogEntry into a tuple of (remoteIP, (1, {set of usernames}))
        This form may seem weird. Now we have a complex value.
        We have two objectives;
            O1) Count the frequency of remoteIPs: we will use the first part of the (1, {username}) for that purpose
            O2) Keep track of used usernames: we will use the second part of the (1, {username}) for that purpose
        (Note: You may notice the set([entry.username]) expression. this is workaround to keep the usernames intact.
        If the username supplied directly to set() function, it would split up each char in the string,
        so for example when we use set(entry.username) for username 'root', we will have a set of {'o', 'r', 't'}
        but when we use set([entry.username]) we will have a set of {'root'})

    2) reduceByKey example is a little bit complicated compare to previous samples;
        Rather than just adding two integers we will dealing with tuples of (frequency, {username, ...})
        As we are dealing with tuples we need to return a tuple as well.
        e.g Assume that we have following entries
        ('192.168.1.1', (5, {'root', 'tomcat'})) , ('192.168.1.1', (10, {'root', 'nagios', 'tomcat'}))
        applying reduceByKey with given lambda function will merge those entries into
        ('192.168.1.1', (15, ['root', 'tomcat', 'nagios']))

        Let's take a look into the given lambda function;
        lambda (remote_frequency1, username_set1), (remote_frequency2, username_set2): (
                    remote_frequency1 + remote_frequency2,  -> adding frequency values with each other
                    username_set1 | username_set2           -> set union operation; merge two sets and generate a new set with unique values
                 )
    """
    sortedRemoteIpUsernameMapping = logEntries\
                                        .map(lambda entry: (entry.remoteIp, (1, set([entry.username]))))\
                                        .reduceByKey(lambda (remote_frequency1, username_set1), (remote_frequency2, username_set2):
                                                             (remote_frequency1 + remote_frequency2, username_set1 | username_set2))

    """
        Save sortedRemoteIpUsernameMapping to disk
        ------------
        1) Save to disk by calling saveAsTextFile
            use os.path.join to concat paths: output_folder + filename
    """
    sortedRemoteIpUsernameMapping.saveAsTextFile(os.path.join(output_folder,"s03-sortedRemoteIpUsernameMapping"))

    """

    ## Sample 04 ## Complex Key, Simple Value

    In previous example we managed to get both remoteIP frequency and usernames used for failed login attempts from
    that remoteIP. Unfortunately we don't have any information about the frequency of usernames. We will calculate both
    the frequency of (remoteIP, username) pairs and will use this information in the next sample.

    Calculate remoteIp & username frequency: remoteIpAndUsernameFrequency
    -------------------------------------------------------
    1) map each LogEntry into a tuple of ((remoteIP,username), 1)
        In this example we use a complex key, rather than having a single value we will use
        a tuple of (remoteIp, username)
    2) by calling reduceByKey we will merge entries by key, the lambda function we provided describes
        how the values need to be merged. In this case, we will simply add the values with each other.
        e.g assume that we have following entries
        (('192.168.1.1', 'root'), 2), (('192.168.1.1', 'root), 5)
        applying reduceByKey with given lambda function will merge those entries into
        (('192.168.1.1', 'root'), 7)
    3) sort the tuples by remoteIP.
        after the reduceByKey transformation we have an unsorted entries in form of ((remoteIP, username), frequency)
        lambda function provided to sortBy transformation will indicate which field should be used for sorting,
        sortBy(lambda x: x[0][0]) -> x[0][0]: remoteIP
                                     x[0][1]: username
     4) Cache the RDD
        We will use this RDD as base for Sample 05.
    """
    remoteIpAndUsernameFrequency = logEntries\
                                        .map(lambda entry: ((entry.remoteIp, entry.username), 1))\
                                        .reduceByKey(lambda frequency1, frequency2: frequency1 + frequency2)\
                                        .sortBy((lambda ((remoteIP, username), frequency): remoteIP), ascending=False)\
                                        .cache()

    """
        Save remoteIpAndUsernameFrequency to disk
        ------------
        1) Save to disk by calling saveAsTextFile
            use os.path.join to concat paths: output_folder + filename
    """
    remoteIpAndUsernameFrequency.saveAsTextFile(os.path.join(output_folder, "s04-remoteIpAndUsernameFrequency"))

    """

    ## Sample 05 ## Join RDDs

    In this sample we will calculate both the remoteIP frequency and username frequency used by this remoteIP.
    In order to do that, first we will process RDD from sample 04 and join it with RDD from sample 02

    To join the RDDs, they have should have similar key structure.

    Transform the RDD from sample 04
    --------------------------------
    1) map the each entry of ((remoteIP, username), frequency) into (remoteIP, (username, frequency)
    2) groupByKey -> will merge the values into an ResultIterable object
    3) mapValues is used to convert ResultIterable objects in arrays.
    """
    usernameFrequencyByRemoteIp = remoteIpAndUsernameFrequency\
                .map(lambda ((remoteIP, username), frequency): (remoteIP, (username, frequency)))\
                                    .groupByKey()\
                                    .mapValues(list)

    usernameFrequencyByRemoteIp.saveAsTextFile(os.path.join(output_folder, "s05-01-usernameFrequencyByRemoteIp"))
    """
    Join the RDDs from sample 02 with 04
    ------------------------------------
    1) join RDD#02 with RDD#05-01
        RDD#02 is in form of (remoteIP, remoteIP_frequency)
        RDD#05-01 is in form of (remoteIP, [(username1, frequency1), (username2, frequency2), ...])
        joining those RDDs will generate RDD#05-02 of form
        (remoteIP, (remoteIP_frequency, [(username1, frequency1), (username2, frequency2), ...]))
    2) sort the RDD by remoteIP frequency.
    3) Save to disk by calling saveAsTextFile
            use os.path.join to concat paths: output_folder + filename
    """
    joinedFrequencies = sortedRemoteIPFrequency.join(usernameFrequencyByRemoteIp)\
                            .sortBy((lambda (remoteIP, (remoteIP_frequency, usernameFrequency)): remoteIP_frequency), ascending=False)

    joinedFrequencies.saveAsTextFile(os.path.join(output_folder, "s05-02-joinedFrequencies"))







    """
    #In this we will calculate both the frequency of (username, remoteIP) pairs and will use this info in the next step.
    Calculate username & remoteIp frequency: remoteUsernameAndIpFrequency
    Each LogEntry is mapped into a tuple of ((username,remoteIP), 1) and finally we will Cache the RDD so that we can use this as base for Sample 06.
    """
    remoteUsernameAndIpFrequency = logEntries\
                                        .map(lambda entry: ((entry.username, entry.remoteIp), 1))\
                                        .reduceByKey(lambda frequency1, frequency2: frequency1 + frequency2)\
                                        .sortBy((lambda ((username, remoteIP), frequency): username), ascending=False)\
                                        .cache()
    """
    #Sample 06 #Join RDDs
    In this, we will calculate both the username frequency and remoteIP frequency, used by this username.
    So, to do that, first we will process RDD from above and join it with RDD from sample 01
    Transform the above RDD and then map each entry of ((username, remoteIP), frequency) into (username, (remoteIP, frequency))
    """

    RemoteIpFrequencyByusername = remoteUsernameAndIpFrequency\
                .map(lambda ((username, remoteIP), frequency): (username, (remoteIP, frequency)))\
                                    .groupByKey()\
                                    .mapValues(list)

    RemoteIpFrequencyByusername.saveAsTextFile(os.path.join(output_folder, "s06-01-RemoteIpFrequencyByusername"))

    """
    join RDD#01 with RDD#06-01 and then sort the RDD by usernameFrequency.

    """


    joinedFrequencies_username = sortedUsernameFrequency.join(RemoteIpFrequencyByusername)\
                            .sortBy((lambda (username, (usernameFrequency, remoteIP_frequency)): usernameFrequency), ascending=False)

    joinedFrequencies_username.saveAsTextFile(os.path.join(output_folder, "s06-02-joinedFrequencies"))

