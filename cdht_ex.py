#COMP3331 Programming Assignment 15s2
#By Raphael Rueda, z3461774

import socket
import time
import select
import sys

#====GLOBALS====#

#Connection vars
host = "127.0.0.1"
basePort = 50000

#Network peer information
id = ""
succ1 = ""
succ2 = ""
pred1 = ""
pred2 = ""

#Global sockets
udp = ""
tcpR = ""

#Status information, stc
pingTime = 7
quitOK = False
seqNo1 = 0
seqNo2 = 0
pingTrack1 = []
pingTrack2 = []
timeout1 = ""
timeout2 = ""

#==GLOBALS END==#

#=====INIT======#
def init():
    #Set peer information
    global id
    global succ1
    global succ2    
    id = sys.argv[1]
    succ1 = sys.argv[2]
    succ2 = sys.argv[3]

    #Set UDP socket
    global udp
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind((host, basePort + int(id)))
    udp.setblocking(0)
     
    #Set TCP listener socket
    global tcpR
    tcpR = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpR.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcpR.bind((host, basePort + int(id)))
    tcpR.listen(5)
    tcpR.setblocking(0)

#    print "Bound to: " + str((host, basePort + int(id)))
#=================#

#==UDP/PING HANDLER==#
def receiveUDP():
    global pred1
    global pred2
    global pingTrack1
    global pingTrack2
    #Get data
    data, addr = udp.recvfrom(1024)
    splitData = data.split(" ")
    if len(splitData) == 4:		#Ping request handler ... [1] = sequence number, [2] = from, [3] = predecessor depth
        if splitData[0] == "p-req":
            print "A ping request message was received from Peer " + splitData[2] + "."
            pingRes(splitData[2], splitData[1])
	    if splitData[3] == "1":
		pred1 = splitData[2]
	    elif splitData[3] == "2":
		pred2 = splitData[2]
    elif len(splitData) == 3:		#Ping response handler ... [1] = sequence number, [2] = from
	if splitData[0] == "p-res":
            print "A ping response message was received from Peer " + splitData[2] + "."
	    if int(splitData[2]) == int(succ1):
		if int(splitData[1]) in pingTrack1:
		    pingTrack1.remove(int(splitData[1]))
		    cleanTrack(1, int(splitData[1]))
	    elif int(splitData[2]) == int(succ2):
		if int(splitData[1]) in pingTrack2:
		    pingTrack2.remove(int(splitData[1]))
		    cleanTrack(2, int(splitData[1]))
    else:				#Unrecognised data handler
	print "Received corrupt data: " + data

#Ping request sender
def pingReq(to, depth):
    if int(to) == int(succ1):
	global seqNo1
	global pingTrack1
	udp.sendto("p-req " + str(seqNo1) + " " + str(id) + " " + str(depth), (host, basePort + int(to)))
#	print "Removing " + splitData[1] + " from pingTrack1."
	pingTrack1.append(seqNo1)
	seqNo1 += 1
    elif int(to) == int(succ2):
	global seqNo2
	global pingTrack2
	udp.sendto("p-req " + str(seqNo2) + " " + str(id) + " " + str(depth), (host, basePort + int(to)))
	pingTrack2.append(seqNo2)
#	print "Appending " + str(seqNo2) + " to pingTrack2."
	seqNo2 += 1

#Ping response sender
def pingRes(to, seqNo):
    udp.sendto("p-res " + str(seqNo) + " " + str(id), (host, basePort + int(to)))
	
#Ping track cleaner ~> ensures that unanswered pings are consecutive and clears those that aren't
def cleanTrack(trackNo, seqNo):
    global pingTrack1
    global pingTrack2
    if trackNo == 1:
	pingTrack1 = [x for x in pingTrack1 if not (x < seqNo)]
    elif trackNo == 2:
	pingTrack2 = [x for x in pingTrack2 if not (x < seqNo)]

#=================#

#===TCP HANDLER===#
def receiveTCP():
    global succ1
    global succ2
    global pred1
    global pred2
    global pingTrack1
    global pingTrack2
    global seqNo1
    global seqNo2
    global quitOK
    conn, addr = tcpR.accept()
    data = conn.recv(1024)
    splitData = data.split(" ")
    if data == "quitOK":
	if quitOK == False: 	#first quitOK notice
	    quitOK = True
	else:			#second quitOK notice
	    sys.exit("Predecessors confirmed quit. Exiting now.")
    #Successor request
    elif data == "succ-req":		
	conn.send(succ1)
    elif len(splitData) == 3:	# [0] = request/response, [1] = filename, [2] = requester/fileowner
	#File request
	if splitData[0] == "f-req":
	    findFile(splitData[1], splitData[2])
	#File response
	elif splitData[0] == "f-res":
	    print "Received a response message from peer " + splitData[2] + ", which has the file " + splitData[1] + "."
    #Successor/Predecessor updater (after quit)
    elif len(splitData) == 4:	# [0] = succ/pred, [1] = leaving peer, [2] = new succ1/pred1, [3] = new succ2/pred2
	if splitData[0] == "q-suc":
	    print "Peer " + splitData[1] + " will depart from the network."
	    if int(splitData[1]) == int(succ1):
		pingTrack1 = []
		seqNo1 = 0
	    elif int (splitData[1]) == int(succ2):
		pingTrack2 = []
		seqNo2 = 0
	    succ1 = splitData[2]
	    succ2 = splitData[3]
	    print "My first successor is now peer " + succ1 + "."
	    print "My second successor is now peer " + succ2 + "."	    

	    sendQuitOK(splitData[1])
	elif splitData[0] == "q-pre":
	    pred1 = splitData[2]
	    pred2 = splitData[3]
    #Unrecognised data handler
    else:
	print "Received corrupt data: " + data
    conn.close()

#Sends quit confirmation
def sendQuitOK(to):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(to)))
    tcp.send("quitOK")
    tcp.close()
    
#===FILE HANDLER===#

#File locator
def findFile(fileName, requester):
    #Compute hash
    hash = hashFile(fileName)
    #Check if we have file
    if int(hash) >= int(id) and (int(hash) < int(succ1) or int(id) > int(succ1)):
        print "File " + fileName + " is here."
        if requester != id:
            print "A response message, destined for peer " + requester + ", has been sent."
            sendFileRes(fileName, requester)
    #If not, pass request to successor
    else:
        print "File " + fileName + " is not stored here."
        sendFileReq(fileName, requester)

#Hashes filename
def hashFile(fileName):
    return ((int(fileName) + 1) % 256)

#File request sender
def sendFileReq(fileName, requester):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(succ1)))
    tcp.send("f-req " + fileName + " " + requester)
    print "File request message has been forwarded to my successor."
    tcp.close()

#File response sender
def sendFileRes(fileName, requester):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(requester)))
    tcp.send("f-res " + fileName + " " + id)
    tcp.close()



#====QUIT HANDLER====#
def notifyQuit():
    #tell first predecessor
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(pred1)))
    #send msg listing first predecessors new successors
    tcp.send("q-suc " + id + " " + succ1 + " " + succ2)
    tcp.close()

    #tell second predecessor
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(pred2)))
    #send msg listing second predecessors new successors
    tcp.send("q-suc " + id + " " + pred1 + " " + succ1)
    tcp.close()

    #update successors predecessors
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(succ1)))
    #send msg listing second predecessors new successors
    tcp.send("q-pre " + id + " " + pred1 + " " + pred2)
    tcp.close()

    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect((host, basePort + int(succ2)))
    #send msg listing second predecessors new successors
    tcp.send("q-pre " + id + " " + succ1 + " " + pred1)
    tcp.close()

#==UNGRACEFUL QUIT HANDER==#
def ungraceQuit(quitter):
    global succ1
    global succ2
    global pingTrack1
    global pingTrack2
    global seqNo1
    global seqNo2
    #Successor 1 quit
    if quitter == 1:	
	print "Peer " + succ1 + " is no longer alive."
	succ1 = succ2
	print "My first successor is now peer " + succ1 + "."
	tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	tcp.connect((host, basePort + int(succ1)))
	tcp.send("succ-req")
	succ2 = tcp.recv(1024)
	print "My second successor is now peer " + succ2 + "."
	tcp.close()
	pingTrack1 = []
	pingTrack2 = []
	seqNo1 = 0
	seqNo2 = 0
    #Successor 2 quit
    else:
	print "Peer " + succ2 + " is no longer alive."
	print "My first successor is now peer " + succ1 + "."
	tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	tcp.connect((host, basePort + int(succ1)))
	tcp.send("succ-req")
	succ2 = tcp.recv(1024)
	print "My second successor is now peer " + succ2 + "."
	tcp.close()
	pingTrack2 = []
	seqNo2 = 0

#MAIN
def run():
    global timeout1
    global timeout2
    init()
    while True:
	#Read from stdin
	if select.select([sys.stdin,], [],[],0.0)[0]:
	    input = sys.stdin.readline()
	    input = input.rstrip()
	    splitInput = input.split(" ")
	    if len(splitInput) == 2:
		if splitInput[0] == "request":
		    findFile(splitInput[1], id)
	    elif input == "quit":
		notifyQuit()
	    else:
		print "<" + input.rstrip() + ">"  + " is not a recognised command."
	sockets = [tcpR,udp]
	#Poll the udp and tcp listeners for a ready socket
	inputReady, outputReady, exceptReady = select.select(sockets, [], [], 0.0)
	for s in inputReady:
	    if s == udp:	#UDP ready
		receiveUDP()
	    elif s == tcpR:
		receiveTCP()	#TCP ready
	if int(time.time()) % pingTime == 0:	#Ping frequency
	    pingReq(succ1, 1)
	    pingReq(succ2, 2)
	    time.sleep(1)
	if len(pingTrack1) > 2:		#If 3 or more consecutive pings unanswered
	    if timeout1 == "":
		timeout1 = float(time.time())
	    if (float(time.time()) - float(timeout1)) > 4: #Then wait 4 seconds to determine an ungraceful quit
		ungraceQuit(1)
	else:
	    timeout1 = ""
        if len(pingTrack2) > 3:		#Similar to above, with tweaked numbers for second successor
            if timeout2 == "":
                timeout2 = float(time.time())
            if (float(time.time()) - float(timeout2)) > 5:
                ungraceQuit(2)
        else:
            timeout2 = ""

if __name__ == "__main__":
    run()
