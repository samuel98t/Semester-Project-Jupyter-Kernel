import zmq
import jupyter_client
import sys
import json
import uuid
import threading
import hmac
import hashlib
import datetime
import juliacall
from juliacall import Main as jl
from juliacall import Base as jlbase
import io
import traceback




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
        # Initialize execution count
        self.execution_count = 0
        # Initialize PyJulia
        jl.seval('using InteractiveUtils')


    

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
    def validate_signature(self, message_parts):
        if len(message_parts) < 6:
            return False
        received_signature_str = message_parts[1].decode()
        if received_signature_str.startswith("sha256="):
            received_signature_str = received_signature_str[len("sha256="):]  # strip prefix

        try:
            received_signature_bytes = bytes.fromhex(received_signature_str)
        except ValueError:
            return False

        # Our connection key from the JSON file, ensure it's bytes
        key = self.connection_info["key"]
        if isinstance(key, str):
            key = key.encode("utf-8")

        # Prepare an HMAC object
        h = hmac.new(key, digestmod=hashlib.sha256)

        # Update HMAC with the signable parts: header, parent_header, metadata, content
        for part in message_parts[2:]:
            h.update(part)

        # Compare
        expected_signature = h.digest()
        if hmac.compare_digest(expected_signature, received_signature_bytes):
            return True
        else:
            return False

            
    # Recieves the message from the specified channel and
    # cuts it in to the relevant parts to handle it 
    # accordingly to its type
    def handle_message(self,socket_name,socket):
        message = socket.recv_multipart()
        # DEBUGGING
        print(f"Raw message received on {socket_name}: {message}")  
        zmq_identities = message[:-6]  # The ZMQ identities are all parts *before* the last 6 (delimiter, signature, headers, content)
        delimiter = message[-6]
        signature = message[-5]
        header_bytes = message[-4]
        parent_header_bytes = message[-3]
        metadata_bytes = message[-2]
        content_bytes = message[-1]
        # Validate the signature
        if not self.validate_signature([delimiter, signature, header_bytes, parent_header_bytes, metadata_bytes, content_bytes]):
            print(f"Signature validation failed on {socket_name}")
            return

        header = json.loads(header_bytes)
        parent_header = json.loads(parent_header_bytes)
        metadata = json.loads(metadata_bytes)
        content = json.loads(content_bytes)
        msg_type = header['msg_type']

        # FOR DEBUGGING
        print(f"Received message of type: {msg_type} on {socket_name} socket")

        #TODO HANDLE MESSAGE ACCORDING TO ITS TYPE.
        # Deal with execute request messages
        if msg_type == 'execute_request':
            self.handle_execute_request(socket_name, socket, header, parent_header, metadata, content, zmq_identities)
        # Deal with kernel info request message
        elif msg_type == 'kernel_info_request':
            self.handle_kernel_info_request(socket_name, socket, header, zmq_identities)


    def sign_message(self, header_str, parent_header_str, metadata_str, content_str):
        # Get the key from our connection_info and make sure its in bytes.
        key = self.connection_info["key"]
        if isinstance(key, str):
            key = key.encode("utf-8")

        # Create a new HMAC object
        h = hmac.new(key, digestmod=hashlib.sha256)
        # Update the HMAC with the message parts
        h.update(header_str.encode())
        h.update(parent_header_str.encode())
        h.update(metadata_str.encode())
        h.update(content_str.encode())
        # Return the signature
        return h.hexdigest()

# Sends a response message to the client using the selected socket
    def send_response(self, socket_name, socket, msg_type, content, parent_header=None, metadata=None, zmq_identities=None):
        # Get the socket object
        socket_obj = getattr(self, f"{socket_name}_socket")

        header = {
            "msg_id": str(uuid.uuid4()),
            "session": self.session_id,
            "username": "kernel",
            "date": datetime.datetime.now().isoformat(),
            "msg_type": msg_type,
            "version": "5.3",
        }
        if parent_header is None:
            parent_header = {}
        if metadata is None :
            metadata = {}

        # Turn the message parts to JSON
        header_str = json.dumps(header)
        parent_header_str = json.dumps(parent_header)
        metadata_str = json.dumps(metadata)
        content_str = json.dumps(content)
        # Calculate the signature of the message
        signature = self.sign_message(header_str, parent_header_str, metadata_str, content_str)

        parts = [
            b"<IDS|MSG>",
            signature.encode(),
            header_str.encode(),
            parent_header_str.encode(),
            metadata_str.encode(),
            content_str.encode(),
        ]
        if zmq_identities:
            socket_obj.send_multipart(zmq_identities + parts)
        else:
            socket_obj.send_multipart(parts)

    
    # Here all the code execution magic should happen, taking the code, identifying language,
    # executing it, sending back responses if theres a need, or output/errors etc...
    def handle_execute_request(self, socket_name, socket, header, parent_header, metadata, content, zmq_identities):
        self.send_iopub_status("busy",header)
        code = content['code']
        self.execution_count += 1 # increment the exectution count

        # send response
        input_content = {
            'code': code,
            'execution_count': self.execution_count,
        }
        self.send_response('iopub', None, 'execute_input', input_content, parent_header=header)

        # Language detection using magics
        lines = code.split('\n')
        line_1 = lines[0].strip()
        if line_1.startswith("%julia"):
            language = "julia"
            code_to_exec = '\n'.join(lines[1:])
        elif line_1.startswith("%python"):
            language = "python"
            code_to_exec = '\n'.join(lines[1:])
        else:
            # Default to python if there is no magic command
            language = "python"
            code_to_exec = code
            # DEBUGGING
        print(f"Detected language: {language}")
        print(f"Code to execute: {code_to_exec}")
        # Execute the code and handle errors

        # Prepare to catch output/errors
        captured_stdout = io.StringIO()
        captured_stderr = io.StringIO()

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        output = ""
        error_output = ""

        # Try to execute code according to the selected language.
        try :
            if language == "julia":
                return
            elif language == "python":
                sys.stdout = captured_stdout
                sys.stderr = captured_stderr
                try:
                    exec(code_to_exec,globals(),locals())
                except Exception:
                    error_output = traceback.format_exc()
                    output = ""
                finally:
                    # Restore for python aswell
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
            output = captured_stdout.getvalue()
            error_output = captured_stderr.getvalue()


        except Exception as e:
        # General exception handling 
            error_output = traceback.format_exc()
            output = ""
        finally:
            # restore always after
            sys.stdout = old_stdout
            sys.stderr = old_stderr
        
        # Publish output (if theres any)
            if output:
               output_content = {
                'output_type': 'stream',
                'name': 'stdout',
                'text': output,
               }
               self.send_response('iopub', None, 'stream', output_content, parent_header=header)
        # Publish error output (if theres any)
            if error_output:
                error_content = {
                'output_type': 'stream',
                'name': 'stderr',
                'text': error_output,
            }
                self.send_response('iopub', None, 'stream', error_content, parent_header=header)
            
            # Send execute result on shell channel
            execute_reply_content = {
                'status': 'ok' if not error_output else 'error',
                'execution_count': self.execution_count,
                'payload': [],  # List of display data.
                'user_expressions': {},
            }
           
            if error_output:
                execute_reply_content['ename'] = 'Error'
                execute_reply_content['evalue'] = str(error_output).splitlines()[-1]
                execute_reply_content['traceback'] = str(error_output).splitlines()
            
            self.send_response(socket_name, socket, 'execute_reply', execute_reply_content, parent_header=header, zmq_identities=zmq_identities)
            self.send_iopub_status("idle",header)


    # This sends busy/idle status
    def send_iopub_status(self, status_string, parent_header):
        content = {'execution_state': status_string }
        self.send_response('iopub',None,'status',content,parent_header=parent_header)

    # this handles unimportant message requests and thier replies.
    def handle_extra_messages(self, socket_name, socket, header, parent_header, metadata, content, zmq_identities):
        msg_type = header["msg_type"]

        # Send "busy" status 
        self.send_iopub_status("busy", header)
        if msg_type == "history_request":
            reply_content = {"history": []}
            self.send_response(socket_name, socket, "history_reply", reply_content,parent_header=header,zmq_identities=zmq_identities)

        elif msg_type == "comm_info_request":
            # Must reply with comm_info_reply
            reply_content = {"comms": {}}
            self.send_response(socket_name, socket, "comm_info_reply", reply_content,parent_header=header,zmq_identities=zmq_identities)

        elif msg_type in ("comm_open", "comm_msg", "comm_close"):
            pass

        else:
        # Catch any other extra messages 
            print(f"Warning: Unhandled extra msg_type: {msg_type}")

        # Send "idle" status 
        self.send_iopub_status("idle", header)       


            

    # Function to handle info request 
    def handle_kernel_info_request(self, socket_name, socket, header, zmq_identities):
        # Send busy to iopub
        self.send_iopub_status("busy", header)
        self.send_response('iopub',None,'status',{'execution_state': 'busy'},parent_header=header)
        # DEBUGGING
        print("Handling kernel_info_request...")
        print(f"Socket name: {socket_name}")
        print(f"ZMQ Identities: {zmq_identities}")
        reply_content = {
            'status': 'ok',
            'protocol_version': '5.3', 
            'implementation': 'JuliaPythonKernel', 
            'implementation_version': '0.1.0', 
            'language_info': {
                'name': 'julia and python',
                'version': str(jl.eval('VERSION')) if hasattr(jl, 'eval') else sys.version.split()[0] ,
                'mimetype': 'text/x-python',
                'file_extension': '.ipynb',
                'pygments_lexer': 'python3',
                'codemirror_mode': {'name': 'ipython', 'version': 3},
            },
            'banner': 'JuliaPythonKernel - A Jupyter kernel for executing Julia and Python code.',
            'help_links': [
                {'text': 'MyJuliaKernel Documentation', 'url': 'https://github.com/samuel98t/Semester-Project-Jupyter-Kernel'}  # my github link
            ]
        }
        # DEBUGGING
        print("Sending kernel_info_reply...",zmq_identities)
        self.send_response(socket_name, socket, 'kernel_info_reply', reply_content, parent_header=header, zmq_identities=zmq_identities)
        # Send Idle response 
        self.send_iopub_status("idle", header)



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

                # Handle messages on both sockets using the same handler
                if self.shell_socket in sockets:
                    self.handle_message("shell", self.shell_socket)

                if self.control_socket in sockets:
                    self.handle_message("control", self.control_socket)

            except zmq.error.ZMQError as e:
                if e.errno == zmq.ETERM:
                    print("ZMQ context terminated. Exiting main loop.")
                    break
                else:
                    raise


if __name__ == "__main__":
        
    connection_file_path = sys.argv[1]
    # Create the kernel
    kernel = Kernel(connection_file_path)
    # Start the kernel's main loop
    kernel.start()




