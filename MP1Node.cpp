/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include "sstream"
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "initthisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 777;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     * Clean up these pointers
     * EmulNet *emulNet;
     Log *log;
     Params *par;
     Member *memberNode;
     *
     * Clean up memberlist
     *
     */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilities!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     * handle JOINREQ, JOINREP, DUMMYLASTMSGTYPE
     * To determine message type, check introducer IP against own IP, with respect to IP in message.
     * update memberlist based on DUMMYLASTMSGTYPE
     */
    int requestType = *data;
    long timestamp = (long) time(NULL);
    cout << "recvCallBack msgType:" << requestType << endl;
    cout << "msgsize: " << size << endl;

    if (requestType == 0) {
        //JOINREQ
        //Add node to memberlist and return memberlist in char array.

        //MessageHdr *repMsg;
        //Build memberlist values from *data
        long heartbeat = *(long *) (data + 5 + sizeof(memberNode->addr.addr));

        //Populate the addr with joining member addr values starting at data+1
        Address addr;
        int id = *(int *) (data + 1);
        short port = *(short *) (data + 5);
        memcpy(&addr.addr[0], &id, sizeof(int));
        memcpy(&addr.addr[4], &port, sizeof(short));
        cout << "Joiner Address: ";
        printAddress(&addr);

        //If memberlist is empty, add yourself.
        if (memberNode->memberList.size() == 0) {
            MemberListEntry *IntroMLE = new MemberListEntry((int) memberNode->addr.addr[0],
                                                            (short) memberNode->addr.addr[4], memberNode->heartbeat,
                                                            timestamp);
            memberNode->memberList.push_back(*IntroMLE);
            memberNode->myPos = memberNode->memberList.begin();
        }

        //Build MLE from joiner data
        MemberListEntry *joinerMLE = new MemberListEntry((int) addr.addr[3], (short) addr.addr[4], heartbeat,
                                                         timestamp);
        //Add it to memberlist
        memberNode->memberList.push_back(*joinerMLE);

        //Build return message
        //Create a string from the memberlist
        string list = to_string(1);
        for (int i = 0; i < memberNode->memberList.size(); i++) {
            list = list + "," + to_string(memberNode->memberList[i].getid()) + ":" +
                   to_string(memberNode->memberList[i].getport()) + ":" +
                   to_string(memberNode->memberList[i].getheartbeat()) + ":" +
                   to_string(memberNode->memberList[i].gettimestamp());
        }
        cout << "MemberList String: " << list << endl;

        //Build the JoinRep message
        //repMsg->msgType = JOINREP;
        char *repMsg = new char[list.size() + 1];
        std::copy(list.begin(), list.end(), repMsg);
        repMsg[list.size()] = '\0'; // don't forget the terminating 0

        //Build msgsize
        size_t msgsize = sizeof(repMsg);

        //Send the JoinRep message
        emulNet->ENsend(&memberNode->addr, &addr, repMsg, msgsize);

        // don't forget to free the string after finished using it
        free(repMsg);

        return 1;
    }

    if (requestType == 1) {
        //JOINREP
        //Change char pointer into string then parse into memberlist

        //Convert data back into a string
        string joinRepData(data);
        cout << "joinrep data: " << joinRepData << endl;

        //Build a vector of the membernodes in joinRepData
        vector<string> members;
        split(joinRepData,',',members);

        //Build and populate the memberlist
        vector<string> tempMle;
        for (int i = 1; i < members.size(); i++) {
            split(members[i],':',tempMle);
            memberNode->memberList[i].setid(stoi(tempMle[0]));
            memberNode->memberList[i].setport((short)stoi(tempMle[1]));
            memberNode->memberList[i].setheartbeat(stol(tempMle[2]));
            memberNode->memberList[i].settimestamp(stol(tempMle[3]));
            tempMle.clear();
        }

        //Set myPos
        memberNode->myPos = memberNode->memberList.end()++;

        return 1;
    }

    if (requestType == 2) {
        //GOSSIP
        //Change char array into string and parse into temp memberlist, then compare against current memberlist
        string gossipData(data);
        cout << "gossipData: " << gossipData << endl;

        //Build a vector of the membernodes in gossipData
        vector<string> members;
        split(gossipData,',',members);

        //Build and populate the temp memberlist
        vector<string> tempMle;
        vector<MemberListEntry> tempMemList;
        for (int i = 1; i < members.size(); i++) {
            split(members[i],':',tempMle);
            tempMemList[i].setid(stoi(tempMle[0]));
            tempMemList[i].setport((short)stoi(tempMle[1]));
            tempMemList[i].setheartbeat(stol(tempMle[2]));
            tempMemList[i].settimestamp(stol(tempMle[3]));
            tempMle.clear();
        }

        //See if there are any nodes in the tempMemList that the membernode doesn't have, then add them.
        //TODO: this may need to change to a MemberListEntry assignment operator instead of a push_back
        if ((tempMemList.size()-1) > memberNode->memberList.size()) {
            int missingNodes = (int)(tempMemList.size()-1) - (int)memberNode->memberList.size();
            for (int i=1; i < missingNodes; i++){
                memberNode->memberList.push_back(tempMemList[memberNode->memberList.size()+i]);
            }
        }

        //Now compare heartbeats of each node between memberlists, and update heartbeat and timestamp accordingly.
        for (int i=0; i < memberNode->memberList.size(); i++){
            if (tempMemList[i+1].getheartbeat() > memberNode->memberList[i].getheartbeat()){
                memberNode->memberList[i].setheartbeat(tempMemList[i+1].getheartbeat());
                memberNode->memberList[i].settimestamp(timestamp);
            }
        }

        return 1;

    }

return 0;

}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
    void MP1Node::nodeLoopOps() {

        /*
         * Your code goes here
         * Pull member list
         * Cleanup any nodes in member list past T_cleanup
         * Update own heartbeat and TS
         * select members to ping
         * do ENsend to selected members
         */

        return;
    }

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
    int MP1Node::isNullAddress(Address *addr) {
        return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
    }

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
    Address MP1Node::getJoinAddress() {
        Address joinaddr;

        memset(&joinaddr, 0, sizeof(Address));
        *(int *) (&joinaddr.addr) = 1;
        *(short *) (&joinaddr.addr[4]) = 0;

        return joinaddr;
    }

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
    void MP1Node::initMemberListTable(Member *memberNode) {
        memberNode->memberList.clear();
    }

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
    void MP1Node::printAddress(Address *addr) {
        printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
               addr->addr[3], *(short *) &addr->addr[4]);
    }

    /**
     * FUNCTION NAME: split
     *
     * DESC: Needed something to quickly split strings on delim.
     */
    void MP1Node::split(const string &s, char delim, vector<string> &elems) {
        stringstream ss;
        ss.str(s);
        string item;
        while (getline(ss, item, delim)) {
            elems.push_back(item);
        }
    }



