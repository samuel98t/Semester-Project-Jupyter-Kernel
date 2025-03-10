import zmq
import sys
import json
import uuid
import threading
import hmac
import hashlib
import datetime
from juliacall import Main as jl
from juliacall import Base as jlbase
import io
import traceback
import builtins
from multiprocessing import Process, Pipe
import base64

# small helper func to help with types for bridging
def get_julia_value(varname):
    val_jl = jl.seval(varname)
    # if its julia dict
    if jl.isa(val_jl, jl.Dict):
        py_dict = {}
        for (k, v) in val_jl.items():
            key_str = str(k)  # ensure it's a string
            if jl.isa(v, jl.Array):
                eltype = jl.Base.eltype(v)
                if str(eltype) == "Char":
                    py_dict[key_str] = "".join(str(x) for x in v)
                else:
                    py_dict[key_str] = list(v)
            elif jl.isa(v, jl.Dict):
                py_dict[key_str] = dict(v)  #
            else:
                py_dict[key_str] = v
        return py_dict

    # if its an array
    if jl.isa(val_jl, jl.Array):
        eltype = jl.Base.eltype(val_jl)
        if str(eltype) == "Char":
            # convertt
            return "".join(str(ch) for ch in val_jl)
        else:
            # array to list
            return list(val_jl)

    # treat as scalar or string
    return val_jl

def python_worker(child_conn):
    import re
    import rlcompleter
    env = {"__builtins__": __builtins__}
    while True:
        message = child_conn.recv()
        if message.get("command") == "exec":
            execution_id = message.get("execution_id")
            code = message.get("code", "")

            # this will override display function for this execution
            # define display to use the current sys.displayhook
            def display(value):
                custom_displayhook(value)
            env["display"] = display

            def custom_displayhook(value):
                # store last output
                mime_bundle = {}
                builtins._ = value
                if hasattr(value,'_repr_html_'):
                    mime_bundle["text/html"] = value._repr_html_()
                # png case
                elif hasattr(value,'_repr_png_'):
                    png_data = value._repr_png_()
                    # encode to base64 just incase
                    b64 = base64.b64encode(png_data).decode('ascii')
                    mime_bundle["image/png"] = b64
                # jpeg case
                elif hasattr(value, '_repr_jpeg_'):
                    jpeg_data = value._repr_jpeg_()
                    b64 = base64.b64encode(jpeg_data).decode('ascii')
                    mime_bundle["image/jpeg"] = b64
                # svg case
                elif hasattr(value, '_repr_svg_'):
                    svg_data = value._repr_svg_()
                    # SVG is text, so no need to encode.
                    mime_bundle["image/svg+xml"] = svg_data
                # default case
                else:
                    mime_bundle["text/plain"] = repr(value)
                # send the output using the pipe
                child_conn.send({
                    "type": "display_data",
                    "execution_id": execution_id,
                    "data":mime_bundle,
                    "metadata":{}
                })
            # overwrite display hook
            original_displayhook = sys.displayhook
            sys.displayhook = custom_displayhook
            # overrides input
            def custom_input(prompt=''):
                child_conn.send({
                    "type": "input_request",
                    "execution_id": execution_id,
                    "prompt": prompt
                })
                # wait for input response
                resp = child_conn.recv()
                if (resp.get("type") == "input_response" and
                        resp.get("execution_id") == execution_id):
                    return resp.get("value", "")
                else:
                    return ""
            original_input = builtins.input
            builtins.input = custom_input

            # redirect stdout/stderr to capture them
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()

            try:
                # execute 
                exec(code, env, env)
                output = sys.stdout.getvalue()
                error_output = sys.stderr.getvalue()
                child_conn.send({
                    "type": "result",
                    "execution_id": execution_id,
                    "status": "ok",
                    "output": output,
                    "error": error_output
                })
            except Exception:
                error_output = traceback.format_exc()
                output = sys.stdout.getvalue()
                child_conn.send({
                    "type": "result",
                    "execution_id": execution_id,
                    "status": "error",
                    "output": output,
                    "error": error_output
                })
            finally:
                # restore
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                builtins.input = original_input
                sys.displayhook = original_displayhook

        elif message.get("command") == "shutdown":
            break
        elif message.get("command") == "complete":
            code = message.get("code","")
            cursor_pos = message.get("cursor_pos",len(code))
            # create a completer for the current enviorment
            py_completer = rlcompleter.Completer(namespace=env)
            # find the token being completed
            token_match = re.search(r"[\w\.]+$", code[:cursor_pos])
            token = token_match.group(0) if token_match else ""
            # use rlcompleter to get matches for our token
            matches = []
            if token:
                i = 0
                while True:
                    match = py_completer.complete(token,i)
                    if match is None:
                        break
                    matches.append(match)
                    i += 1
            # determine start & end of token
            if token_match:
                # position relative 
                cursor_start = code[:cursor_pos].rindex(token)
                cursor_end = cursor_start + len(token)
            else:
                cursor_start = cursor_pos
                cursor_end = cursor_pos
            # Send the completion results back to the kernel
            child_conn.send({
                "type": "complete_result",
                "matches": matches,
                "cursor_start": cursor_start,
                "cursor_end": cursor_end,
                "metadata": {},
                "status": "ok",
            })
        elif message.get("command") == "get_vars":
            requested_vars = message.get("variables", [])
            results = {}
            for varname in requested_vars:
                if varname in env:
                    value = env[varname]
                    try:
                        serialized = json.dumps(value)
                    except:
                        # if not json serialziable
                        serialized = None
                    results[varname] = serialized
            
            child_conn.send({
                "type": "vars_data",
                "variables": results
            })
        elif message.get("command") == "set_vars":
            
            data = message.get("data", {})
            for varname, val_json in data.items():
                if val_json is not None:
                    try:
                        val = json.loads(val_json)
                    except:
                        val = None
                    env[varname] = val
           
            child_conn.send({
                "type": "set_vars_ok"
            })

def julia_worker(child_conn):
    # for variable bridging 
    jl.seval("""
    if !isdefined(Main, :temp_val)
        global temp_val = nothing
    end
    """)
    # streaming func for input/stderr/stoudt
    jl.seval(r"""
module MyStreaming
    using PythonCall
    using REPL
    using REPL.REPLCompletions
    using Base: showerror, catch_backtrace, invokelatest, pushdisplay, popdisplay
    using Base64
    import Base: display
    # function to get completions
    function my_repl_completer(code::String, cursor_pos::Int)
         comps = REPL.REPLCompletions.completions(code, cursor_pos)
         matches = comps[1]
         (cursor_start, cursor_end) = comps[2]
         # Convert each completion to string whilte stripping extra text 
        matches_str = [
            isa(m, REPL.REPLCompletions.KeywordCompletion) ? m.keyword :
            isa(m, REPL.REPLCompletions.ModuleCompletion)  ? string(m.mod) :
            string(m)
            for m in matches
        ]
         return matches_str, cursor_start, cursor_end
    end

    # function to capture rich data
    function capture_mime(result)::Dict{String,Any}
        d = Dict{String,Any}()

        # Try text/html
        let io = IOBuffer()
            if showable(MIME("text/html"), result)
                show(io, MIME("text/html"), result)
                str = String(take!(io))
                if !isempty(str)
                    d["text/html"] = str
                end
            end
        end

        # Try image/svg+xml
        let io = IOBuffer()
            if showable(MIME("image/svg+xml"), result)
                show(io, MIME("image/svg+xml"), result)
                str = String(take!(io))
                if !isempty(str)
                    d["image/svg+xml"] = str
                end
            end
        end

        # Try image/png (binary, so base64-encode)
        let io = IOBuffer()
            if showable(MIME("image/png"), result)
                show(io, MIME("image/png"), result)
                data = take!(io)
                if !isempty(data)
                    d["image/png"] = base64encode(data)
                end
            end
        end
        # Try JPEG
        let io = IOBuffer()
            if showable(MIME("image/jpeg"), result)
                show(io, MIME("image/jpeg"), result)
                data = take!(io)
                if !isempty(data)
                    d["image/jpeg"] = base64encode(data)
                end
            end
        end
        # If we found nothing , use basic default 
        if isempty(d)
            let io = IOBuffer()
                show(io, MIME("text/plain"), result)
                str = String(take!(io))
                if !isempty(str)
                    d["text/plain"] = str
                else
                    d["text/plain"] = repr(result)
                end
            end
        end
        return d
    end
    # Define custom display
    struct JupyterDisplay <: Base.AbstractDisplay
        callback::Any
    end
    # method of one argument display
    function Base.display(d::JupyterDisplay, x)
        mime_bundle = capture_mime(x)
        if !isempty(mime_bundle)
            d.callback(mime_bundle, "display_data")
        end
        return nothing
    end
    # method for 2 argument display
    function Base.display(d::JupyterDisplay, mime::AbstractString, content)
        if mime in ["image/png", "image/jpeg"] && isa(content, AbstractVector{UInt8})
            #encode if raw btyes
            content = base64encode(content)
        end
        mime_bundle = Dict(mime => content)
        d.callback(mime_bundle, "display_data")
        return nothing
    end
    # global ref to current active display
    const active_jupyter_display = Ref{Union{JupyterDisplay, Nothing}}(nothing)

    function display(mime::AbstractString, content)
        if active_jupyter_display[] !== nothing
            #use the active display
            display(active_jupyter_display[], mime, content)
        else
            # fallback.
            println(content)
            return nothing
        end
        return nothing
    end
    # defineing stream
    struct StreamingOutput <: IO
         callback::Any
         name::String
    end

    # Should work for both string/ substring
    function Base.write(io::StreamingOutput, s::Union{String, SubString{String}})
         io.callback(s, io.name)
         return length(s)
    end

    # Single byte takin
    function Base.write(io::StreamingOutput, b::UInt8)
         io.callback(string(Char(b)), io.name)
         return 1
    end

    function Base.flush(io::StreamingOutput)
         return nothing
    end

    const input_cb = Ref{Any}(nothing)
    function set_input_callback(cb::Any)
         input_cb[] = cb
    end

    mutable struct JupyterStdin <: IO
         input_buffer::IOBuffer
         JupyterStdin() = new(IOBuffer())
    end
    const jupyter_stdin = JupyterStdin()

    function Base.readline(io::JupyterStdin)
         if input_cb[] === nothing
              throw(ArgumentError("No input callback is set."))
         end
         line = input_cb[]("")  # Call the Python callback
         write(io.input_buffer, string(line) * "\n")
         seekstart(io.input_buffer)
         return String(take!(io.input_buffer))
    end

    function Base.read(io::JupyterStdin, ::Type{UInt8})
         if eof(io.input_buffer)
              return UInt8(0)
         end
         return read(io.input_buffer, UInt8)
    end

    function streaming_eval(code::String, cb::Any)
         pysys = pyimport("sys")
         orig_stdout_write = pysys.stdout.write
         orig_stderr_write = pysys.stderr.write
        # keep for restoring
         old_stdout = Base.stdout
         old_stderr = Base.stderr
         old_stdin = Base.stdin
        # wrap the code for multiline handling
         code_wrapped = "begin\n" * code * "\nend"

         # create our streaming stdout/stderr
         streaming_stdout = StreamingOutput(cb, "stdout")
         streaming_stderr = StreamingOutput(cb, "stderr")

         try
              pysys.stdout.write = x -> begin
                   cb(x, "stdout")
                   nothing
              end
              pysys.stderr.write = x -> begin
                   cb(x, "stderr")
                   nothing
              end
              # override
              Base.stdout = streaming_stdout
              Base.stderr = streaming_stderr
              Base.stdin = jupyter_stdin
            # use custom display to capture cells
            jdisp = JupyterDisplay(cb)
            active_jupyter_display[] = jdisp  # set the active display
            pushdisplay(jdisp)
              result = Core.eval(Main, Meta.parse(code_wrapped))
              # check for rich display
              if result !== nothing
                 # attempt capturing
                 mimebundle = invokelatest(capture_mime,result)
                 if mimebundle isa Dict{String, Any} && !isempty(mimebundle) 
                    # callback
                    cb(mimebundle, "display_data")
                 end
              end
         catch e
              io = IOBuffer()
              showerror(io, e, catch_backtrace())
              cb(String(take!(io)), "stderr")
         finally
              # restore
              Base.stdout = old_stdout
              Base.stderr = old_stderr
              Base.stdin = old_stdin
              # restore display aswell
              popdisplay()
              active_jupyter_display[] = nothing

              pysys.stdout.write = orig_stdout_write
              pysys.stderr.write = orig_stderr_write
         end
    end
end
""")
    while True:
        # same as python process
        message = child_conn.recv()
        if message.get("command") == "exec":
            execution_id = message.get("execution_id")
            code = message.get("code", "")
            # the callback to send to python using the pipe
            def julia_input_callback(prompt):
                child_conn.send({
                    "type": "input_request",
                    "execution_id": execution_id,
                    "prompt": prompt
                })
                resp = child_conn.recv()
                if (resp.get("type") == "input_response" and
                        resp.get("execution_id") == execution_id):
                    return resp.get("value", "")
                else:
                    return ""
            jl.MyStreaming.set_input_callback(julia_input_callback)
            def julia_print_callback(chunk, stream_name):
                # If we see "display_data"
                if stream_name == "display_data" :
                    # Check if chunk is a julia dict and if so convert to python
                    if jl.isa(chunk, jl.Dict):
                        data = {}
                        # iter over it
                        for key in jl.keys(chunk):
                            py_key = str(key)
                            value = chunk[key]
                            # Convert 
                            if jl.isa(value, jl.String):
                                data[py_key] = str(value)
                            else:
                            # Handle other types 
                                data[py_key] = value
                        #  Send as display_data
                        child_conn.send({
                            "type": "display_data",
                            "execution_id": execution_id,
                            "data": data,
                            "metadata": {}
                            })
                    else:
                        # Handle non-dict display data (unlikely case)
                        child_conn.send({
                            "type": "stream",
                            "execution_id": execution_id,
                            "stream": "stdout",
                            "text": str(chunk)
                            })
                else:

                    if not isinstance(chunk,str):
                        chunk = str(chunk)
                    # send chunk 
                    child_conn.send({
                    "type": "stream",
                    "execution_id": execution_id,
                    "stream": stream_name,
                    "text": chunk
                    })
            try:
                jl.MyStreaming.streaming_eval(code, julia_print_callback)
                child_conn.send({
                    "type": "result",
                    "execution_id": execution_id,
                    "status": "ok",
                    "output": "",  
                    "error": ""
                })
            except Exception:
                error_output = traceback.format_exc()
                child_conn.send({
                    "type": "result",
                    "execution_id": execution_id,
                    "status": "error",
                    "output": "",
                    "error": error_output
                })
        elif message.get("command") == "shutdown":
            break
        elif message.get("command") == "complete":
            code = message.get("code", "")
            cursor_pos = message.get("cursor_pos", len(code))
            try:
                # call the julia completer function
                result = jl.MyStreaming.my_repl_completer(code, cursor_pos)
                completions = list(result[0])  # convert to python list
                cursor_start = int(result[1])-1  # makes sure these are ints
                cursor_end = cursor_pos

                child_conn.send({
                    "type": "complete_result",
                    "matches": completions,
                    "cursor_start": cursor_start,
                    "cursor_end": cursor_end,
                    "metadata": {},
                    "status": "ok"
                })
            # error , i didnt do one for python tho
            except Exception:
                error_output = traceback.format_exc()
                child_conn.send({
                    "type": "complete_result",
                    "matches": [],
                    "cursor_start": cursor_pos,
                    "cursor_end": cursor_pos,
                    "metadata": {},
                    "status": "error",
                    "error": error_output
            })
        elif message.get("command") == "get_vars":
            requested_vars = message.get("variables", [])
            results = {}
            for varname in requested_vars:
                try:
                    val_py = get_julia_value(varname)  # Use the function above
                    
                    # Then do JSON:
                    serialized = json.dumps(val_py)
                except Exception as e:
                    
                    serialized = None
                results[varname] = serialized
            child_conn.send({"type": "vars_data", "variables": results})
        elif message.get("command") == "set_vars":
            data = message.get("data", {})
            for varname, val_json in data.items():
                val_py = json.loads(val_json) if val_json else None
                jl.Main.temp_val = val_py
                jl.seval(f"global {varname} = temp_val")
                # clear temp
                jl.seval("global temp_val = nothing")
            child_conn.send({"type": "set_vars_ok"})

# read_connection_file is a function that takes a filepath to the connection file,
#  opens its content and parses it and then returns it.
def read_connection_file(filepath):
    try:
        with open(filepath,'r') as f:
            connection_info = json.load(f) 
            return connection_info
    # deal with file not being found.    
    except FileNotFoundError:
        
        sys.exit(1)
    # deal with JSON file being invalid.
    except json.JSONDecodeError:
        
        sys.exit(1)


class Kernel:
    # init function to take in connection file and create the sockets.
    def __init__(self,connection_file):
        self.connection_file = connection_file
        self.connection_info = read_connection_file(connection_file)
        self.context = zmq.Context()
        self.waiting_for_input = False
        self._input_result = ""
        # flag for interuption
        self.interrupted = False
        # track current langaue here
        self.current_language = None
        self.input_event = threading.Event()
        # Setup the sockets.
        self.shell_socket, self.iopub_socket, self.stdin_socket, self.control_socket, self.hb_socket = self.setup_sockets()
        # Setup the id.
        self.session_id = str(uuid.uuid4())
        # Initialize execution count
        self.execution_count = 0
        # init python enviorment
        self.execution_env = {}
        # create the processes /pipes
        self.python_parent_conn, python_child_conn = Pipe()
        self.julia_parent_conn, julia_child_conn = Pipe()
        self.python_process = Process(target=python_worker, args=(python_child_conn,))
        self.julia_process = Process(target=julia_worker, args=(julia_child_conn,))
        self.python_process.start()
        self.julia_process.start()





    
    # the briding between 2 lang variables function
    def bridge_func(self,lang,var_list):
        # ask python for varibles.
        if lang == "py2jl":
            request = {
            "command": "get_vars",
            "variables": var_list
            }
            self.python_parent_conn.send(request)
            vars_data = {}
            # wait for response
            while True:
                msg = self.python_parent_conn.recv()
                if msg.get("type") == "vars_data":
                    vars_data = msg.get("variables", {})  
                    break
            # now pass that to julia worker
            request_julia = {
            "command": "set_vars",
            "data": vars_data  
            }
            self.julia_parent_conn.send(request_julia)

            # Wait for "set_vars_ok"
            while True:
                msg = self.julia_parent_conn.recv()
                if msg.get("type") == "set_vars_ok":
                    # bridging complete
                    return
        # same thing here but for jl2py
        elif lang == "jl2py":
            # send to julia
            self.julia_parent_conn.send({
            "command": "get_vars",
            "variables": var_list
            })
            vars_data = {}
            while True:
                # wait for response
                msg = self.julia_parent_conn.recv()
                if msg.get("type") == "vars_data":
                    vars_data = msg.get("variables", {})
                    break
        
            # send to python
            self.python_parent_conn.send({
                "command": "set_vars",
                "data": vars_data
                })
            while True:
                msg = self.python_parent_conn.recv()
                if msg.get("type") == "set_vars_ok":
                    #done 
                    return

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
        zmq_identities = message[:-6]  # The ZMQ identities are all parts *before* the last 6 (delimiter, signature, headers, content)
        delimiter = message[-6]
        signature = message[-5]
        header_bytes = message[-4]
        parent_header_bytes = message[-3]
        metadata_bytes = message[-2]
        content_bytes = message[-1]
        # Validate the signature
        if not self.validate_signature([delimiter, signature, header_bytes, parent_header_bytes, metadata_bytes, content_bytes]):
            return

        header = json.loads(header_bytes)
        parent_header = json.loads(parent_header_bytes)
        metadata = json.loads(metadata_bytes)
        content = json.loads(content_bytes)
        msg_type = header['msg_type']


        # Deal with execute request messages
        if msg_type == 'execute_request':
            self.handle_execute_request(socket_name, socket, header, parent_header, metadata, content, zmq_identities)
        # Deal with kernel info request message
        elif msg_type == 'kernel_info_request':
            self.handle_kernel_info_request(socket_name, socket, header, zmq_identities)
        elif msg_type == 'input_reply':
            self.handle_input_reply(content)
        elif msg_type == 'shutdown_request':
            self.handle_shutdown_request(socket_name,socket,header,parent_header,metadata,content,zmq_identities)
        elif msg_type == "interrupt_request":
            self.handle_interrupt_request(socket_name, socket, header, parent_header, metadata, content, zmq_identities)
        elif msg_type == "complete_request":
            self.handle_complete_request(socket_name, socket, header, parent_header, metadata, content, zmq_identities)
        else:
        # Send all other message types to handle_extra_messages:
            self.handle_extra_messages(
            socket_name, 
            socket, 
            header, 
            parent_header, 
            metadata, 
            content, 
            zmq_identities
        )

    def handle_extra_messages(self, socket_name, socket, header, parent_header, metadata, content, zmq_identities):
        msg_type = header["msg_type"]

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

    # function to send display data
    def send_display_data(self, execution_id, data, metadata, parent_header, zmq_identities=None):
        formatted_data = {}
    
        for mime_type, content in data.items():
            formatted_data[mime_type] = content
        content = {
            "data": formatted_data,       
            "metadata": metadata or {},
            "transient": {},
            }
        self.send_response('iopub', None, 'display_data', content, parent_header=parent_header, zmq_identities=zmq_identities)


    # handles shutdown of kernel
    def handle_shutdown_request(self,socket_name,socket,header,parent_header,metadata,content,zmq_identites):
        # False by default
        restart = content.get('restart',False)
        reply_content = {'restart': restart}
        self.send_response(socket_name,socket,'shutdown_reply',reply_content,parent_header = header,zmq_identities=zmq_identites)
        self.shutdown()
    # handles restarting worker when getting interrupt
    def restart_worker(self, language):
        if language == "python":
            print("Restarting Python worker...")
            try:
                self.python_process.terminate()
                self.python_process.join(timeout=1)
                if self.python_process.is_alive():
                    self.python_process.kill()
                    self.python_process.join()
            except Exception as e:
                print("Error terminating Python worker:", e)
            # create new pipe + worker
            self.python_parent_conn, python_child_conn = Pipe()
            self.python_process = Process(target=python_worker, args=(python_child_conn,))
            self.python_process.start()
            print("Python worker restarted.")
        elif language == "julia":
            print("Restarting Julia worker...")
            try:
                self.julia_process.terminate()
                self.julia_process.join(timeout =1)
                if self.julia_process.is_alive():
                    self.julia_process.kill()
                    self.julia_process.join()
            except Exception as e:
                print("Error terminating Julia worker:", e)
            # create new pipe + worker
            self.julia_parent_conn, julia_child_conn = Pipe()
            self.julia_process = Process(target=julia_worker, args=(julia_child_conn,))
            self.julia_process.start()
            print("Julia worker restarted.")

    # shutdown happens here
    def shutdown(self):
        print("Shutting down kernel...")
        # close the processes aswell
        self.python_parent_conn.send({"command": "shutdown"})
        self.julia_parent_conn.send({"command": "shutdown"})
        # close the sockets
        self.shell_socket.close()
        self.iopub_socket.close()
        self.stdin_socket.close()
        self.control_socket.close()
        self.hb_socket.close()
        self.context.term()
        sys.exit(0)
    # handles interrupt requests while in code execution
    def handle_interrupt_request(self, socket_name, socket, header, parent_header, metadata, content, zmq_identities): 
        self.interrupted = True  # set to True
        if self.current_language == "python":
            self.restart_worker("python")
        if self.current_language == "julia":
            self.restart_worker("julia")

        self.current_language = None # reset it after using it
        reply_content = {}  
        self.send_response(socket_name, socket, 'interrupt_reply', reply_content, parent_header=header, zmq_identities=zmq_identities)

    # Here all the code execution magic should happen, taking the code, identifying language,
    # executing it, sending back responses if theres a need, or output/errors etc...
    def handle_execute_request(self, socket_name, socket, header, parent_header, metadata, content, zmq_identities):
        self.send_iopub_status("busy",header)
        code = content['code']
        self.execution_count += 1 # increment the exectution count
        self.interrupted = False 

        # send response
        input_content = {
            'code': code,
            'execution_count': self.execution_count,
        }
        self.send_response('iopub', None, 'execute_input', input_content, parent_header=header)

        # Language detection using magics
        lines = code.split('\n')
        line_1 = lines[0].strip()
        # Default to python if there is no magic command
        language = "python"
        code_to_exec = code
        # added bridging logic to move basic variables between langauges
        bridge = False 
        if line_1.startswith("%py2jl") or line_1.startswith("%jl2py"):
            bridge = True
            if line_1.startswith("%py2jl"):
                language = "julia"
                direction = "py2jl"
                magic = "%py2jl"
            else:
                language = "python"
                direction = "jl2py"
                magic = "%jl2py"
            # collect variable names from the whole cell
            first_line_extra = line_1[len(magic):].strip()
            var_text = first_line_extra
            if len(lines) > 1:
                var_text += " " + " ".join(line.strip() for line in lines[1:] if line.strip())
            # split the variables
            var_list = [v.strip() for v in var_text.replace(',', ' ').split() if v.strip()]
            self.bridge_func(direction, var_list)
            # this cell is just for bridging no need to execute code.
            code_to_exec = ""
        elif line_1.startswith("%julia"):
            language = "julia"
            code_to_exec = '\n'.join(lines[1:])
        elif line_1.startswith("%python"):
            language = "python"
            code_to_exec = '\n'.join(lines[1:])
        self.current_language = language
        execution_id = self.execution_count
        # send to the correct process
        if language == "julia" and not(bridge):
            self.julia_parent_conn.send({"command": "exec", "code": code_to_exec, "execution_id": execution_id})
            pipe = self.julia_parent_conn
        elif language == "python" and not(bridge):
            self.python_parent_conn.send({"command": "exec", "code": code_to_exec, "execution_id": execution_id})
            pipe = self.python_parent_conn
        # Spawn a helper thread to wait for the result and process input requests.
        if not(bridge):
            threading.Thread(target=self.wait_for_result, args=(pipe, execution_id, header, zmq_identities)).start()
        # send execute reply after bridging so kernel can continue working
        if bridge:
            execute_reply_content = {
                'status': 'ok',
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {}
            }
            # send it on the "shell" socket
            self.send_response(
                'shell',
                self.shell_socket,
                'execute_reply',
                execute_reply_content,
                parent_header=header,
                zmq_identities=zmq_identities
            )
            # also idle
            self.send_iopub_status("idle", header)
        bridge = False

    # function to handle complete request (autocomplete with TAB key)
    def handle_complete_request(self,socket_name,socket,header,parent_header,metadata,content,zmq_identities):
        self.send_iopub_status("busy", header) # send busy
        # extrcats the code and cursor position from content
        code = content.get("code","")
        cursor_pos = content.get("cursor_pos",len(code))
        # first figure out the cell langauge
        def detect_cell_language(code):
            if not code.strip():
                return "python"  # default
            first_line = code.splitlines()[0].strip()
            if first_line.startswith("%julia"):
                return "julia"
            elif first_line.startswith("%python"):
                return "python"
            else:
                return "python"  # default if no magic is present
        # python case
        if detect_cell_language(code) == "python":
            # send complete command to python worker
            self.python_parent_conn.send({
                "command": "complete",
                "code": code,
                "cursor_pos": cursor_pos
            })
            # wait for reply
            while True:
                msg = self.python_parent_conn.recv()
                if msg.get("type") == "complete_result":
                    # get matches+ positions
                    matches = msg["matches"]
                    cursor_start = msg["cursor_start"]
                    cursor_end = msg["cursor_end"]

                    # send the reply content as a msg to frontend
                    reply_content = {
                        "matches": matches,
                        "cursor_start": cursor_start,
                        "cursor_end": cursor_end,
                        "metadata": {},
                        "status": msg["status"]  # "ok"
                    }
                    self.send_response(socket_name, socket, "complete_reply",reply_content, parent_header=header, zmq_identities=zmq_identities)
                break
        else:
            # send to julia process
            self.julia_parent_conn.send({
                "command": "complete",
                "code": code,
                "cursor_pos": cursor_pos
                })
            # wait for reply
            while True:
                msg = self.julia_parent_conn.recv()
                if msg.get("type") == "complete_result":
                    reply_content = {
                        "matches": msg["matches"],
                        "cursor_start": msg["cursor_start"],
                        "cursor_end": msg["cursor_end"],
                        "metadata": {},
                        "status": msg["status"],
                    }
                    self.send_response(socket_name, socket, "complete_reply", reply_content, parent_header=header, zmq_identities=zmq_identities)
                    break

        self.send_iopub_status("idle", header) # send idle



    def handle_julia_output(self, text, parent_header,name = "stdout"):
        if not text:
            return
        content = {
        "output_type": "stream",  
        "name": name,
        "text": text,
        }
        self.send_response(
        socket_name='iopub',
        socket=None,
        msg_type='stream',
        content=content,
        parent_header=parent_header
        )
    def send_iopub_status(self, status_string, parent_header):
        content = {'execution_state': status_string }
        self.send_response('iopub',None,'status',content,parent_header=parent_header)
        
    def wait_for_result(self, pipe, execution_id, header, zmq_identities):
        result = None
        try:
            # Loop to handle input/results and stream messages
            while True:
                msg = pipe.recv()
                if msg.get("type") == "input_request" and msg.get("execution_id") == execution_id:
                    prompt = msg.get("prompt", "")
                    self.waiting_for_input = True
                    self.input_event.clear()
                    self.send_input_request(prompt, False, header, zmq_identities)
                    self.input_event.wait()  # Block until input_reply is received.
                    input_value = self._input_result
                    pipe.send({"type": "input_response", "execution_id": execution_id, "value": input_value})
                    self.waiting_for_input = False
                elif msg.get("type") == "stream" and msg.get("execution_id") == execution_id:
                    # foward the stream msgs to iopub
                    stream_name = msg.get("stream")
                    text = msg.get("text")
                    output_content = {
                    'output_type': 'stream',
                    'name': stream_name,
                    'text': text,
                    }
                    self.send_response('iopub', None, 'stream', output_content, parent_header=header, zmq_identities=zmq_identities)
                elif msg.get("type") == "result" and msg.get("execution_id") == execution_id:
                    result = msg
                    break
                elif msg.get("type") == "display_data" and msg.get("execution_id") == execution_id:
                    mime_bundle = msg.get("data")
                    metadata = msg.get("metadata", {})
                    self.send_display_data(execution_id, mime_bundle, metadata, parent_header=header, zmq_identities=zmq_identities)

        except (EOFError, BrokenPipeError):
        # for interrupt.
            print("Worker pipe closed. Exiting wait_for_result thread.")
            self.send_iopub_status("idle", header)
            execute_reply_content = {
            'status': 'abort',
            'execution_count': self.execution_count,
            'payload': [],
            'user_expressions': {},
            }
            self.send_response('shell', self.shell_socket, 'execute_reply', execute_reply_content,
                           parent_header=header, zmq_identities=zmq_identities)

            return

        # If we got a result, process it.
        if result is None:
            return

        output = result.get("output", "")
        error = result.get("error", "")
        if output:
            output_content = {
            'output_type': 'stream',
            'name': 'stdout',
            'text': output,
            }
            self.send_response('iopub', None, 'stream', output_content, parent_header=header, zmq_identities=zmq_identities)
        if error:
            error_content = {
            'output_type': 'stream',
            'name': 'stderr',
            'text': error,
            }
            self.send_response('iopub', None, 'stream', error_content, parent_header=header, zmq_identities=zmq_identities)
        status = result.get("status", "ok")
        execute_reply_content = {
        'status': status,
        'execution_count': self.execution_count,
        'payload': [],
        'user_expressions': {},
        }
        if status != "ok":
            execute_reply_content['ename'] = 'Error'
            execute_reply_content['evalue'] = error.splitlines()[-1] if error else ''
            execute_reply_content['traceback'] = error.splitlines() if error else []
        self.send_response('shell', self.shell_socket, 'execute_reply', execute_reply_content, parent_header=header, zmq_identities=zmq_identities)
        self.send_iopub_status("idle", header)      
    
    # function to send input request.
    def send_input_request(self, prompt, password, parent_header, zmq_identities=None):
        content = {
            'prompt': prompt,
            'password': password
        }
        self.send_response('stdin', None, 'input_request', content, parent_header=parent_header, zmq_identities=zmq_identities)
    
    # function to handle input reply
    def handle_input_reply(self, content):
        self._input_result = content.get('value', '')
        self.waiting_for_input = False
        self.input_event.set() # unblock

    # Function to handle info request 
    def handle_kernel_info_request(self, socket_name, socket, header, zmq_identities):
        self.send_response('iopub',None,'status',{'execution_state': 'busy'},parent_header=header)
        reply_content = {
            'status': 'ok',
            'protocol_version': '5.3', 
            'implementation': 'JuliaPythonKernel', 
            'implementation_version': '0.1.0', 
            'language_info': {
                'name': 'python',
                'version': '3.13' ,
                'mimetype': 'text/x-python',
                'file_extension': '.py',
                'pygments_lexer': 'python3',
                'codemirror_mode': 'ipython',
            },
            'banner': 'JuliaPythonKernel - A Jupyter kernel for executing Julia and Python code.',
            'help_links': [
                {'text': 'MyJuliaKernel Documentation', 'url': 'https://github.com/samuel98t/Semester-Project-Jupyter-Kernel'}  # my github link
            ]
        }
        self.send_response(socket_name, socket, 'kernel_info_reply', reply_content, parent_header=header, zmq_identities=zmq_identities)
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
            poller.register(self.stdin_socket, zmq.POLLIN) # Stdin for input

            # Poll for events with a timeout 
            timeout = 100 # timeout time in ms
            try:
                sockets = dict(poller.poll(timeout)) 

                # Handle messages on both sockets using the same handler
                if self.shell_socket in sockets:
                    self.handle_message("shell", self.shell_socket)
                if self.control_socket in sockets:
                    self.handle_message("control", self.control_socket)
                if self.stdin_socket in sockets:
                    self.handle_message("stdin", self.stdin_socket)

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




