from proto.commit_protocol_pb2 import *

class twoPhaseCommit:

    def __init__(self, name, messageSendCallback, transactionCompleteCallback):
        self.name = name
        self.messageSendCallback = messageSendCallback
        self.transactionCompleteCallback = transactionCompleteCallback
        self.transaction_data = {}
        self.phase1_replies = {}
        self.phase2_replies = {}

    def prepareCommitReq(self, task, transaction_id, task_id):
        cp_msg = CommitProtocolMessage()
        cp_msg.type = CommitProtocolMessage.COMMIT_REQ
        cp_msg.transaction_id = transaction_id

        creq = CommitReq()
        creq.task.CopyFrom(task)
        creq.task_id = task_id
        cp_msg.commit_req.CopyFrom(creq)
    
        return cp_msg

    def submitTransaction (self, t):
        # Save the transaction
        self.transaction_data[t.transaction_id] = t
        'Step 1: Submit txn - The transaction is a set of tasks. Each task has a server assigned for execution'
        for task in t.tasks:
            commit_req_cp_msg = self.prepareCommitReq(task, t.transaction_id, task.task_id)
            # Update data strctures saying that phase 1 msg was sent to that server
            self.messageSendCallback(task.server, commit_req_cp_msg)

    def start_phase2(self, trans_id):
        print("Starting phase 2")

    def phase2_complete(self, trans_id) :
        if len(self.phase2_replies[trans_id].keys()) == len(self.transaction_data[trans_id].tasks):
            return True
        else:
            return False

    def phase1_complete(self, trans_id) :
        if len(self.phase1_replies[trans_id].keys()) == len(self.transaction_data[trans_id].tasks):
            return True
        else:
            return False

    #CONTROLLER SIDE FUNCTIONS
    def handle_ack(self, trans_id, server, cp_msg):
        print ("Received Ack for trans Id %d from %s" %(trans_id, server))
        if trans_id not in self.phase2_replies:
            self.phase2_replies[trans_id] = {}
        self.phase2_replies[trans_id][server] = cp_msg

        if self.phase2_complete(trans_id):
            self.transactionCompleteCallback(trans_id)

    def handle_agreement(self, trans_id, server, cp_msg):
        print ("Received Agreement for trans Id %d from %s" %(trans_id, server))
        if trans_id not in self.phase1_replies:
            self.phase1_replies[trans_id] = {}
        self.phase1_replies[trans_id][server] = cp_msg

        if self.phase1_complete(trans_id):
            self.start_phase2(trans_id)
   
    def handle_disagreement(self, trans_id, server, cp_msg):
        print ("Received disagreement for trans Id %d from %s" %(trans_id, server))
        if trans_id not in self.phase1_replies:
            self.phase1_replies[trans_id] = {}
        self.phase1_replies[trans_id][server] = cp_msg

        if self.phase1_complete(trans_id):
            self.start_phase2(trans_id)
			
    #SERVER SIDE FUNCTIONS
    def handle_commit(self, trans_id, server, cp_msg):
        print("SERVER: Commit transaction with id %d"%trans_id)

    # For now we just flip a coin to determine if a txn is completed or not, by the time the server gets the commit request from the controller,
    # as that is out of the scope for our project. Being realistic, 
    # success/failure of a transaction would depend on application. Lets assume all transactions are completed successfully in this case	
    def handle_commit_req(self, trans_id, server, cp_msg): 
        print ("SERVER: Commit request has been received")
		#Assuming that transaction has always completed successfully by the time the commit request arrives
        if(True):
            self.txnSuccessCallback(trans_id, server)

    # Txn wasn't completed by server, next step is to disagree to commit request
    def txnFailCallback(self, trans_id, server):
        print("Transaction %d was NOT completed by the server %s"%(trans_id, server))

    # Txn was completed by server, next step is to agree to commit request
    def txnSuccessCallback(self, trans_id, server):
        print("Transaction %d was successfully completed by the server %s"%(trans_id, server))

		
    def handle_abort(self, trans_id, server, cp_msg) : print ("SERVER: Abort transaction with id %d" %trans_id)                             
	
    #GENERAL FUNCTIONS
    def onRcvMsg(self, msg_src, cp_msg):
        msg_type = cp_msg.type
        trans_id = cp_msg.transaction_id
        if msg_type == CommitProtocolMessage.AGREEMENT:
            assert cp_msg.HasField("agreement")
            self.handle_agreement(trans_id, msg_src, cp_msg)
        if msg_type == CommitProtocolMessage.ACK:
            assert cp_msg.HasField("ack")
            self.handle_ack(trans_id, msg_src, cp_msg)
        if msg_type == CommitProtocolMessage.COMMIT_REQ:
            assert cp_msg.HasField("commit_req")
            self.handle_commit_req(trans_id, msg_src, cp_msg)
        if msg_type == CommitProtocolMessage.COMMIT:
            assert cp_msg.HasField("commit")
            self.handle_commit(trans_id, msg_src, cp_msg)
        if msg_type == CommitProtocolMessage.ABORT:
            assert cp_msg.HasField("abort")
            self.handle_abort(trans_id, msg_src, cp_msg)
        if msg_type == CommitProtocolMessage.DISAGREEMENT:
            assert cp_msg.HasField("disagreement")
            self.handle_disagreement(trans_id, msg_src, cp_msg)

def commHandler(server, msg):
    print ("Now I will send a message to server %s"%server)

#def trans_complete(self, trans_id):
def trans_complete(trans_id):
    print ("Transaction %d was complete"%trans_id)

if __name__ == "__main__":

    cp_2pc = twoPhaseCommit("2pc", commHandler, trans_complete)
    
    # Step 1 : controller submits a transaction
    print("######## Testing controller submitting transaction ########")
    trans_id = 1
    trans = Transaction()
    trans.transaction_id = trans_id
    
    num_tasks = 3
    for i in range(num_tasks):
        task = trans.tasks.add()
        task.task_id = i
        task.server = "10.100.1.20%d"%i
        task.task_type = Task.CREATE_APP

    cp_2pc.submitTransaction(trans)
	
	# step 1.5 : server executes transaction
	
	# step 1.75 : controller sends commit request 
    print("######## Testing controller sending commit requests ########")
    for i in range(num_tasks):
        server =  "10.100.1.20%d"%i
        cp_msg = CommitProtocolMessage()
        cp_msg.transaction_id = trans_id
        cp_msg.type = CommitProtocolMessage.COMMIT_REQ
		
        req = CommitReq()
        req.task_id = i
        cp_msg.commit_req.CopyFrom(req)
        cp_2pc.onRcvMsg(server, cp_msg)
		
    # step 2 : server submits an agreement
    print("######## Testing server submitting agreement ########")
    for i in range(num_tasks):
        server =  "10.100.1.20%d"%i
        cp_msg = CommitProtocolMessage()
        cp_msg.transaction_id = trans_id
        cp_msg.type = CommitProtocolMessage.AGREEMENT
        
        agr = Agreement()
        agr.task_id = i
        cp_msg.agreement.CopyFrom(agr)
        cp_2pc.onRcvMsg(server, cp_msg)

    # step 3 : submitting an ack
    for i in range(num_tasks):
        server =  "10.100.1.20%d"%i
        cp_msg = CommitProtocolMessage()
        cp_msg.transaction_id = trans_id
        cp_msg.type = CommitProtocolMessage.ACK
        
        ack = Ack()
        ack.task_id = i
        cp_msg.ack.CopyFrom(ack)
        
        cp_2pc.onRcvMsg(server, cp_msg)