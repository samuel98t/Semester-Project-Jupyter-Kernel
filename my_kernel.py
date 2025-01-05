import zmq
import jupyter_client
import sys
import json
import uuid
import threading
import hmac
import hashlib

# read_connection_file is a function that takes a filepath to the connection file,
#  opens its content and parses it and then returns it.
def read_connection_file(filepath):
    try:
        with open(filepath,'r') as f:
            connection_info = json.load(f) 
            return connection_info
    # deal with file not being found.    
    except FileNotFoundError:
        print(f"Error: Connection file not found, wrong path!")
        sys.exit(1)
    # deal with JSON file being invalid.
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON connection file")
        sys.exit(1)


class Kernel:
    # init function to take in connection file and create the sockets.
    def __init__(self,connection_file):
        self.connection_file = connection_file
        self.connection_info = read_connection_file(connection_file)
        self.context = zmq.Context()
        # Setup the sockets.
        self.shell_socket, self.iopub_socket, self.stdin_socket, self.control_socket, self.hb_socket = self.setup_sockets()
        # Setup the id.
        self.session_id = str(uuid.uuid4())
    

    def setup_sockets(self):
        # Shell socket which is a router type
        shell_socket = self.context.socket(zmq.ROUTER)
        shell_port = self.connection_info["shell_port"]
        shell_socket.bind(f"tcp://{self.connection_info['ip']}:{shell_port}")
        # IOPub socket which is publish type
        iopub_socket = self.context.socket(zmq.PUB)
        iopub_port = self.connection_info["iopub_port"]
        iopub_socket.bind(f"tcp://{self.connection_info['ip']}:{iopub_port}")
        # Stdin socket which is a router type
        stdin_socket = self.context.socket(zmq.ROUTER)
        stdin_port = self.connection_info["stdin_port"]
        stdin_socket.bind(f"tcp://{self.connection_info['ip']}:{stdin_port}")
        # Control socket which is a router type
        control_socket = self.context.socket(zmq.ROUTER)
        control_port = self.connection_info["control_port"]
        control_socket.bind(f"tcp://{self.connection_info['ip']}:{control_port}")
        # Heartbeat socket which is a rep type.
        hb_socket = self.context.socket(zmq.REP)
        hb_port = self.connection_info["hb_port"]
        hb_socket.bind(f"tcp://{self.connection_info['ip']}:{hb_port}")
        # return the newly created sockets
        return shell_socket, iopub_socket, stdin_socket, control_socket, hb_socket
    
    # simple ping-pong function for heartbeat
    def handle_heartbeat(self):
        while True:
            message = self.hb_socket.recv()
            self.hb_socket.send(message)
    
    # makes sure the message was not tampered with 
    def validate_signature(self, message):
        delimiter, received_signature = message[:2]
        # Convert the recived signature from hex to bytes
        received_signature_bytes = bytes.fromhex(received_signature.decode())
        # Get the key from connection info and make sure its in bytes.
        key = self.connection_info["key"]
        if isinstance(key, str):
            key = key.encode("utf-8")
        # Create then Update the hmac object with the parts except delimiter and signature
        h = hmac.new(key, digestmod=hashlib.sha256)
        for part in message[2:]:
            h.update(part)
        # Compare the expected signature with the one in the message ,
        # If matching message is secure 
        expected_signature = h.digest()
        if hmac.compare_digest(expected_signature, received_signature_bytes):
            return True
        else:
            return False


    # Recieves the message from the shell channel and
    # cuts it in to the relevant parts to handle it 
    # accordingly to its type
    def handle_shell_message(self):
        message = self.shell_socket.recv_multipart()
        # Validate the signature
        if not self.validate_signature(message):
            return
        delimiter = message[0]
        signature = message[1]
        header = json.loads(message[2])
        parent_header = json.loads(message[3])
        metadata = json.loads(message[4])
        content = json.loads(message[5])
        msg_type = header['msg_type']

        #TODO : Handle the message according to its type.

    # Recieves the message from the control channel and
    # cuts it in to the relevant parts to handle it 
    # accordingly to its type
    def handle_control_message(self):
        message = self.control_socket.recv_multipart()
        # Validate the signature
        if not self.validate_signature(message):
            return
        delimiter = message[0]
        signature = message[1]
        header = json.loads(message[2])
        parent_header = json.loads(message[3])
        metadata = json.loads(message[4])
        content = json.loads(message[5])
        msg_type = header['msg_type']

        #TODO : Handle the message according to its type.
    
    
    # this starts the kernel's main loop.
    def start(self):

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.handle_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # Main loop
        while True:
            # use zmq.Poller to mointor the sockets 
            poller = zmq.Poller()
            poller.register(self.shell_socket, zmq.POLLIN) # Shell
            poller.register(self.control_socket, zmq.POLLIN) # Control

            # Poll for events with a timeout 
            timeout = 100 # timeout time in ms
            try:
                sockets = dict(poller.poll(timeout)) 

                # Handle Shell socket messages
                if self.shell_socket in sockets:
                    self.handle_shell_message()

                # Handle Control socket messages
                if self.control_socket in sockets:
                    self.handle_control_message()

            except zmq.error.ZMQError as e:
                if e.errno == zmq.ETERM:
                    print("ZMQ context terminated. Exiting main loop.")
                    break
                else:
                    raise


if __name__ == "__main__":
        
    connection_file_path="connection.json"
    # Create the kernel
    kernel = Kernel(connection_file_path)
    # Start the kernel's main loop
    kernel.start()




