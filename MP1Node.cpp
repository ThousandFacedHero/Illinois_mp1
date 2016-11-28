/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

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
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->memberList);
        // size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->memberList, sizeof(memberNode->memberList));
        //memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        //memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
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
     * handle JOINREQ, JOINREP, GOSSIP
     */
    int requestType = *data;
    cout << "recvCallBack msgType:" << requestType<< endl;
    cout<<"recvCallBack message size : "<<size<<endl;
    //Check message type and send to corresponding function
    if(requestType==0){
        recvJoinRequest(env, data, size);
        return 1;
    }

    if(requestType==1){
        recvJoinReply(env, data, size);
        return 1;
    }

    if(requestType==2){
        recvGossip(env, data, size);
        return 1;
    }

    //requestType incorrect, recvcallback failed.
    return 0;
}

/**
 * FUNCTION NAME: recvJoinRequest
 *
 * DESCRIPTION: Add joining node to memberlist,
 *              then send a JOINREP back to that node with current memberlist.
 */
void MP1Node::recvJoinRequest(void *env, char *data, int size){
    MessageHdr *outMsg;
    Address joinerAddr;

    //get incoming memberlist from data
    vector<MemberListEntry> joinerML;
    memcpy(&joinerML, data+1, size-1);

    //should be only one entry in joinerMLE, parse it out into a new entry for current list.
    MemberListEntry joinerMLE = joinerML.back();

    //Add new node to memberlist at the end of the list
    memberNode->memberList.push_back(joinerMLE);

    //Build an Address for joining node
    int joinerId = joinerMLE.getid();
    short joinerPort = joinerMLE.getport();
    memcpy(&joinerAddr.addr[0], &joinerId, sizeof(int));
    memcpy(&joinerAddr.addr[4], &joinerPort, sizeof(short));

    //Log the addition of new node
    log->logNodeAdd(&memberNode->addr, &joinerAddr);
    cout << "Logging joining node: ";
    printAddress(&joinerAddr);

    //Build JoinRep message with current memberlist
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->memberList);
    outMsg = (MessageHdr *) malloc(msgsize * sizeof(char));
    outMsg->msgType = JOINREP;
    memcpy((char *)(outMsg+1), &memberNode->memberList, sizeof(memberNode->memberList));

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, &joinerAddr, (char *)outMsg, msgsize);

    free(outMsg);
}

/**
 * FUNCTION NAME: recvJoinReply
 *
 * DESCRIPTION: Add memberlist from introducer to own initialized memberList.
 */
void MP1Node::recvJoinReply(void *env, char *data, int size) {

}
//Copy memberlist and update myPos with memberlist.end()
/**
 * FUNCTION NAME: recvGossip
 *
 * DESCRIPTION: Compare incoming memberlist to current memberlist,
 *              then update accordingly.
 */
void MP1Node::recvGossip(void *env, char *data, int size){
//For each item in memberlist vector[], run getID to establish a matcher for comparison.
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list to a subset of memberList.
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 * Pull member list
	 * Flag any past T_fail as failed
	 * Cleanup any nodes in member list past T_cleanup
	 * Update own heartbeat and TS
	 * select members to ping
	 * do ENsend with gossip msgtype to selected members
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
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	int id = memberNode->addr.addr[4];
    short port = memberNode ->addr.addr[5];
    long timestamp = (long) time(NULL);
    //Init the List
    memberNode->memberList.clear();
    //Insert yourself to the list
    MemberListEntry *newMLE = new MemberListEntry(id, port, memberNode->heartbeat, timestamp);
    memberNode->memberList.push_back(*newMLE);
    memberNode->myPos = memberNode->memberList.end();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
