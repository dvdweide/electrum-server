import json
import Queue as queue
import socket
import select
import threading
import time
import traceback, sys

from processor import Session, Dispatcher
from utils import print_log


READ_ONLY = select.POLLIN | select.POLLPRI | select.POLLHUP | select.POLLERR
READ_WRITE = READ_ONLY | select.POLLOUT
TIMEOUT = 100

import ssl

class TcpSession(Session):

    def __init__(self, dispatcher, poller, connection, address, use_ssl, ssl_certfile, ssl_keyfile):
        Session.__init__(self, dispatcher)
        self.use_ssl = use_ssl
        self.poller = poller
        self.raw_connection = connection
        if use_ssl:
            import ssl
            self._connection = ssl.wrap_socket(
                connection,
                server_side=True,
                certfile=ssl_certfile,
                keyfile=ssl_keyfile,
                ssl_version=ssl.PROTOCOL_SSLv23,
                do_handshake_on_connect=False)
        else:
            self._connection = connection

        self.address = address[0] + ":%d"%address[1]
        self.name = "TCP " if not use_ssl else "SSL "
        self.timeout = 1000
        self.dispatcher.add_session(self)
        self.response_queue = queue.Queue()
        self.message = ''
        self.retry_msg = ''


    def do_handshake(self):
        if self.use_ssl:
            self._connection.do_handshake()


    def connection(self):
        if self.stopped():
            raise Exception("Session was stopped")
        else:
            return self._connection

    def shutdown(self):
        try:
            self._connection.shutdown(socket.SHUT_RDWR)
        except:
            # print_log("problem shutting down", self.address)
            # traceback.print_exc(file=sys.stdout)
            pass

        self._connection.close()

    def send_response(self, response):
        #print "response", response
        try:
            msg = json.dumps(response) + '\n'
        except:
            return
        self.response_queue.put(msg)
        self.poller.modify(self.raw_connection, READ_WRITE)


    def parse_message(self):

        message = self.message
        self.time = time.time()

        raw_buffer = message.find('\n')
        if raw_buffer == -1:
            return False

        raw_command = message[0:raw_buffer].strip()
        self.message = message[raw_buffer + 1:]
        return raw_command





class TcpServer(threading.Thread):

    def __init__(self, dispatcher, host, port, use_ssl, ssl_certfile, ssl_keyfile):
        self.shared = dispatcher.shared
        self.dispatcher = dispatcher.request_dispatcher
        threading.Thread.__init__(self)
        self.daemon = True
        self.host = host
        self.port = port
        self.lock = threading.Lock()
        self.use_ssl = use_ssl
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile

        self.input_list = []
        self.session_list = {}
        self.buffer_size = 4096





    def handle_command(self, raw_command, session):
        try:
            command = json.loads(raw_command)
        except:
            session.send_response({"error": "bad JSON"})
            return True

        try:
            # Try to load vital fields, and return an error if
            # unsuccessful.
            message_id = command['id']
            method = command['method']
        except KeyError:
            # Return an error JSON in response.
            session.send_response({"error": "syntax error", "request": raw_command})
        else:
            self.dispatcher.push_request(session, command)
            ## sleep a bit to prevent a single session from DOSing the queue
            #time.sleep(0.01)




    def run(self):

        #self.responder = TcpClientResponder(self.shared)
        #self.responder.start()

        print_log( ("SSL" if self.use_ssl else "TCP") + " server started on port %d"%self.port)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setblocking(0)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)

        poller = select.poll()
        poller.register(server)

        # Map file descriptors to socket objects
        fd_to_socket = { server.fileno(): server }
        session_list = {}
        
        while not self.shared.stopped():

            events = poller.poll(TIMEOUT)
            
            for fd, flag in events:

                s = fd_to_socket[fd]
                session = session_list.get(s)
                if not session and s != server:
                    continue

                # handle inputs
                if flag & (select.POLLIN | select.POLLPRI):

                    if s == server:
                        connection, address = s.accept()
                        try:
                            session = TcpSession(self.dispatcher, poller, connection, address, 
                                                 use_ssl=self.use_ssl, ssl_certfile=self.ssl_certfile, ssl_keyfile=self.ssl_keyfile)
                        except BaseException as e:
                            print_log("cannot start TCP session", str(e), address)
                            connection.close()

                        else:
                            try:
                                session.do_handshake()
                            except socket.error:
                                print_log("SSL handshake failure", address)
                                continue

                            connection = session._connection
                            connection.setblocking(0)
                            fd_to_socket[ connection.fileno() ] = connection
                            poller.register(connection, READ_ONLY)
                            session_list[connection] = session

                        continue
                
                    try:
                        data = s.recv(self.buffer_size)
                    except ssl.SSLError as x:
                        if x.args[0] == ssl.SSL_ERROR_WANT_READ: 
                            continue 
                        else: 
                            raise x
                    except socket.error as x:
                        print "recv err", x
                        session.stop()
                        s.close()
                        del session_list[s]
                        continue

                    if data:
                        session.message += data
                        while True:
                            cmd = session.parse_message()
                            if not cmd: 
                                break
                            if cmd == 'quit':
                                data = False
                                break
                            self.handle_command(cmd, session)

                    if not data:
                        poller.unregister(s)
                        session.stop()
                        s.close()
                        del session_list[s]
                        continue

                elif flag & select.POLLHUP:
                    # Client hung up
                    print 'closing', address, 'after receiving HUP'
                    # Stop listening for input on the connection
                    poller.unregister(s)
                    s.close()
                    session.stop()
                    del session_list[s]

                elif flag & select.POLLOUT:
                    # Socket is ready to send data, if there is any to send.
                    if session.retry_msg:
                        next_msg = session.retry_msg
                    else:
                        try:
                            next_msg = session.response_queue.get_nowait()
                        except queue.Empty:
                            # No messages waiting so stop checking for writability.
                            poller.modify(s, READ_ONLY)
                            continue

                    sent = s.send(next_msg)
                    session.retry_msg = next_msg[sent:]


                elif flag & select.POLLERR:
                    print 'handling exceptional condition for', session.address
                    # Stop listening for input on the connection
                    try:
                        poller.unregister(s)
                        s.close()
                    except:
                        pass
                    session.stop()
                    del session_list[s]


    
