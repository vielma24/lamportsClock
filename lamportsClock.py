'''
Author:      Chary Vielma
Description: Algorithm calculate generates a 2D matrix containing Lamport's Logical Clock 
             values for a given a 2D matrix of execution processes containing send, receive, 
             internal and NULL events. 
             Algorithm verify produces a possible execution sequence of events given a 2D
             matrix containing Lamport's Logical Clock values. The 2D output matrix contains
             strings denoting send, receive, internal, and NULL events.
             When a sequence is not achieveable, the algorithm returns 'IMPOSSIBLE'
             Assumptions: Each send event is mapped to only one receive event.
'''

import copy
import string
import itertools
import time
import string
from threading import Lock, Thread
lock = Lock()

global logicalClock
logicalClock = []
global index 
index = 0

def getIndex(value, aList):
    # helper function that finds a given event value in a 2D matrix and returns
    # a list containing tuples of all matches, ex: (rowNum, colNum)
    results = []
    for i, x in enumerate(aList):
        if value in x:
            results.append((i, x.index(value)))
    return results

def verify(n, m, matrix):
    # Assumptions: Each send event is mapped to only one receive event.
    output = [[0 for col in range(m)] for row in range(n)]
    letter = 0
    alphabet = string.ascii_lowercase
    eventNum = 1

    # ensure send event does not come after receive event within the same process
    for sublist in matrix:
        listCopy = filter(lambda a: a != 0, sublist)
        if sorted(listCopy) != listCopy:
            return 'INCORRECT'

    # ensure once first 'NULL' event appears, remaining events in same process are only zeros
    for sublist in matrix:
        try:
            firstNull = sublist.index(0)
            for event in itertools.islice(sublist, firstNull, len(sublist)):
                if event != 0:
                    return 'INCORRECT'
        except:
            pass

    for col in range(m):
        for row in range(n):
            event = matrix[row][col]
            sendList = []
            # null case
            if event == 0:
                output[row][col] = 'NULL'

            # case first column and event is not == 1 or the previous column value does not come immediately before event
            elif (col == 0 and event != 1) or (col != 0 and matrix[row][col-1] != event - 1):

                # generate list of all cells whose event == current event - 1
                sendList = getIndex(event - 1, matrix)

                # if list empty, no matching send event -> exit
                if not sendList:
                    return 'INCORRECT'

                foundSend = False
                for sendRow, sendCol in sendList:
                    # if matrix location not in use, store send event
                    if output[sendRow][sendCol] == 0:
                        output[sendRow][sendCol] = 's' + str(eventNum)  # send event
                        output[row][col] = 'r' + str(eventNum)  # receive event
                        eventNum += 1
                        foundSend = True
                        break

                # if all possible send events are mapped to other receive events, there is no send event for this receive -> exit
                if not foundSend:
                        return 'INCORRECT'

    # generate internal events
    for col in range(m):
        for row in range(n):
            try:
                int(output[row][col])
                output[row][col] = alphabet[letter]
                letter += 1
            except:
                pass

    return output                

def scheduler(aList, myDict):
    global logicalClock, index
    # create copy of dictionary to avoid sharing this resource
    dictCopy = copy.deepcopy(myDict)
    for event in aList:
        cont = True
        eventNum = event[0].lower()

        # internal or send event
        if len(event) == 1 or eventNum == 's':
            lock.acquire() 
            # retrieve send location in 2D array
            row, col = dictCopy[event]
            # if first event, logical clock value == 1 otherwise clock value == value of previous event + 1
            logicalClock[row][col] = 1 if index == 0 else logicalClock[row][col - 1] + 1
            index += 1
            lock.release()

        # receive event
        elif eventNum == 'r':
            # while send has not occured
            while cont:
                if lock.acquire(False):
                    # retrieve send location in 2D array
                    sendRow, sendCol = dictCopy['s' + event[1]]
                    row, col = dictCopy[event]
                    # if send value has not been calculated yet:
                    if logicalClock[sendRow][sendCol] != 0:
                        # if first event, logical clock value == 1 otherwise clock value == value of previous event + 1
                        if index == 0:
                            logicalClock[row][col] = 1
                        else:
                            logicalClock[row][col] = max(logicalClock[row][col - 1], logicalClock[sendRow][sendCol]) + 1 
                        index += 1
                        # send occured and can exit while loop
                        cont = False
                    lock.release()
                else:
                    # send has not occured, check again
                    time.sleep(0.05)

def calculate(rows, cols, matrix):
    global logicalClock
    myDict = {}
    threads = []
    # initialize empty output 2D array to 0's
    logicalClock = [[0 for col in range(cols)] for row in range(rows)]

    # create dictionary key for each event with its corresponding 2D array tuple location value
    for row in range(rows):
        for col in range(cols):
            myDict[matrix[row][col]] = (row, col) 

    # create thread for each process in 2D array
    for process in matrix:
        thread = Thread(target=scheduler, args=(process, myDict))
        threads.append(thread)
        thread.start()

    # wait for all threads to finish execution
    for item in threads:
        item.join()

    return logicalClock

def main():
    # Test case 1
    result = calculate(3, 4, [['a','s1','r3','b'], 
                             ['c','r2','s3','NULL'], 
                             ['r1','d','s2','e']])
    for item in result:
        print item
    print '\n'

    # Test case 2
    result = verify(3, 4, [[1, 2, 8, 9], 
                          [1, 6, 7, 0], 
                          [3, 4, 5, 6]])
    if result == 'INCORRECT':
        print 'INCORRECT'
    else:
        for item in result:
            print item
    print '\n'

    # Test case 3
    result = verify(3, 4, [[1, 2, 8, 9], 
                          [1, 6, 7, 0], 
                          [2, 3, 4, 5]])
    if result == 'INCORRECT':
        print 'INCORRECT'
    else:
        for item in result:
            print item
    print '\n'

    # Test case 4
    result = verify(3, 4, [[1, 2, 8, 9], 
                          [1, 6, 7, 0], 
                          [2, 4, 5, 6]])
    if result == 'INCORRECT':
        print 'INCORRECT'
    else:
        for item in result:
            print item


if __name__ == "__main__":
    main()
