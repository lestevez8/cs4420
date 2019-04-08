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


    #CONTROLLER SIDE FUNCTIONS
    def handle_ack(self, trans_id, server, cp_msg):
        print ("Received Ack for trans Id %d from server %s" %(trans_id, server))
        if trans_id not in self.phase2_replies:
            self.phase2_replies[trans_id] = {}
        self.phase2_replies[trans_id][server] = cp_msg

        if self.phase2_complete(trans_id):
            self.transactionCompleteCallback(trans_id)
			
    def phase1_complete(self, trans_id):
        if(len(self.phase1_replies[trans_id].keys()) == len(self.transaction_data[trans_id].tasks)):
            print("Phase 1 complete")
            return True
        else:
            print("Phase 1 incomplete")
            return False
				
    def handle_agreement(self, trans_id, server, cp_msg):
        print ("Received Agreement for trans Id %d from server %s" %(trans_id, server))
        if trans_id not in self.phase1_replies: self.phase1_replies[trans_id] = {}
        self.phase1_replies[trans_id][server] = CommitProtocolMessage.AGREEMENT
		
        if(len(self.phase1_replies[trans_id].keys()) == len(self.transaction_data[trans_id].tasks)): self.start_phase2(trans_id)
   
    def handle_disagreement(self, trans_id, server, cp_msg):
        print ("Received disagreement for trans Id %d from server %s" %(trans_id, server))
        if(trans_id not in self.phase1_replies): self.phase1_replies[trans_id] = {}
        self.phase1_replies[trans_id][server] = CommitProtocolMessage.DISAGREEMENT

        if(len(self.phase1_replies[trans_id].keys()) == len(self.transaction_data[trans_id].tasks)): self.start_phase2(trans_id)

    
    def start_phase2(self, trans_id):
		# Need to check whether all servers replied with agreement or not
	    transactionSuccess = True
	    for server in self.phase1_replies[trans_id].keys():
	        if(self.phase1_replies[trans_id][server] == CommitProtocolMessage.DISAGREEMENT):
	            transactionSuccess = False
	            print("CONTROLLER: Server %s disagreed to commit. Sending abort order to all servers..."%server)
	        if(self.phase1_replies[trans_id][server] == CommitProtocolMessage.AGREEMENT):
	            print("CONTROLLER: Server %s agreed to commit. Waiting for other servers to respond..."%server)
				
	    if(transactionSuccess): print("CONTROLLER: All servers agreed. Ready to start phase 2")

    def phase2_complete(self, trans_id) :
        if len(self.phase2_replies[trans_id].keys()) == len(self.transaction_data[trans_id].tasks):
            return True
        else:
            return False	
			
    #SERVER SIDE FUNCTIONS
    def handle_commit(self, trans_id, server, cp_msg):
        print("SERVER %s: Received commit order from controller. Commit transaction with id %d" %(server, trans_id))

    def handle_abort(self, trans_id, server, cp_msg): 
        print ("SERVER %s: Received abort order from controller. Abort transaction with id %d" %(server, trans_id))                            

    # For now we just flip a coin to determine if a txn is completed or not, by the time the server gets the commit request from the controller,
    # as that is out of the scope for our project. Being realistic, 
    # success/failure of a transaction would depend on application. Lets assume all transactions are completed successfully in this case	
    def handle_commit_req(self, trans_id, server, cp_msg): 
        print ("SERVER %s: Commit request has been received"%server)
		#Assuming that transaction has always completed successfully by the time the commit request arrives
        if(cp_msg.success == True):
            self.txnSuccessCallback(trans_id, server)
        else: 
            self.txnFailCallback(trans_id, server)

    # Txn wasn't completed by server, next step is to disagree to commit request
    def txnFailCallback(self, trans_id, server):
        print("Transaction %d was NOT completed by the server %s"%(trans_id, server))

    # Txn was completed by server, next step is to agree to commit request
    def txnSuccessCallback(self, trans_id, server):
        print("Transaction %d was successfully completed by the server %s"%(trans_id, server))

			
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

def trans_complete(trans_id):
    print ("Transaction %d was complete"%trans_id)

if __name__ == "__main__":

	num_tasks = 3
	cp_2pc = twoPhaseCommit("2pc", commHandler, trans_complete)
	# TEST CASE 1: Transasction is successful
	print("############################### TEST CASE 1: TRANSACTION IS SUCCESSFUL #################################################")
    
    # Step 1 : controller submits a transaction
	print("######## Testing controller submitting transaction ########")
	trans_id = 1
	trans = Transaction()
	trans.transaction_id = trans_id

	
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
		#Added a new attribute for testing purposes (user will decide whether the txn fails or succeeds for now)
		#req.success = True
		cp_msg.commit_req.CopyFrom(req)
		cp_msg.success = True
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
	
	# step 2.5 : controller checks agreements and commits transaction
	print("####### Testing controller sending commit order to all servers #######")
	
	for i in range(num_tasks):
		server =  "10.100.1.20%d"%i
		cp_msg = CommitProtocolMessage()
		cp_msg.transaction_id = trans_id
		cp_msg.type = CommitProtocolMessage.COMMIT

		com = Commit()
		com.task_id = i
		cp_msg.commit.CopyFrom(com)
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
		
		
		
			
	# TEST CASE 2: Transasction is not successful
	print("############################### TEST CASE 2: TRANSACTION IS UNSUCCESSFUL #################################################")
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
		# Only server 10.100.1.202 will not complete the txn and therefore fail
		cp_msg.commit_req.CopyFrom(req)
		if(i == 2): cp_msg.success = False
		else: cp_msg.success = True
		cp_2pc.onRcvMsg(server, cp_msg)
		
    # step 2 : one server submits a disagreement
	print("######## Testing servers submitting two agreements and a disagreement ########")

	# two agreements
	for i in range(num_tasks - 1):
		server =  "10.100.1.20%d"%i
		cp_msg = CommitProtocolMessage()
		cp_msg.transaction_id = trans_id

		cp_msg.type = CommitProtocolMessage.AGREEMENT
		agr = Agreement()
		agr.task_id = i
		cp_msg.agreement.CopyFrom(agr)
		cp_2pc.onRcvMsg(server, cp_msg)
		
	# one disagreement
	server =  "10.100.1.202"
	cp_msg = CommitProtocolMessage()
	cp_msg.transaction_id = trans_id

	cp_msg.type = CommitProtocolMessage.DISAGREEMENT
	dis = Disagreement()
	dis.task_id = i
	cp_msg.disagreement.CopyFrom(dis)
	cp_2pc.onRcvMsg(server, cp_msg)
	
	# step 2.5 : controller checks agreements and aborts transaction
	print("####### Testing controller sending abort order to all servers #######")
	
	for i in range(num_tasks):
		server =  "10.100.1.20%d"%i
		cp_msg = CommitProtocolMessage()
		cp_msg.transaction_id = trans_id
		cp_msg.type = CommitProtocolMessage.ABORT

		ab = Abort()
		com.task_id = i
		cp_msg.abort.CopyFrom(ab)
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