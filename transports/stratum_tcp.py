import json
import Queue as queue
import socket
import select
import threading
import time
import traceback, sys

from processor import Session, Dispatcher
from utils import print_log


class TcpSession(Session):

    def __init__(self, dispatcher, queue, connection, address, use_ssl, ssl_certfile, ssl_keyfile):
        Session.__init__(self, dispatcher)
        self.use_ssl = use_ssl
        if use_ssl:
            import ssl
            self._connection = ssl.wrap_socket(
                connection,
                server_side=True,
                certfile=ssl_certfile,
                keyfile=ssl_keyfile,
                ssl_version=ssl.PROTOCOL_SSLv23,
                do_handshake_on_connect=True)
        else:
            self._connection = connection

        self.address = address[0] + ":%d"%address[1]
        self.name = "TCP " if not use_ssl else "SSL "
        self.timeout = 1000
        self.dispatcher.add_session(self)
        self.response_queue = queue
        self.message = ''


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
        self.response_queue.put((self,response))



class TcpClientResponder(threading.Thread):

    def __init__(self, shared):
        threading.Thread.__init__(self)
        self.shared = shared
        self.response_queue = queue.Queue()

    def run(self):

        while not self.shared.stopped():

            try:
                session, response = self.response_queue.get(timeout=1)
            except queue.Empty:
                continue

            data = json.dumps(response) + "\n"
            try:
                while data:
                    l = session._connection.send(data)
                    data = data[l:]
            except:
                print "error during send", session.address
                session.stop()





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
        self.delay = 0.0001
        self.buffer_size = 4096


    def on_accept(self):
        try:
            connection, address = self.s.accept()
        except:
            traceback.print_exc(file=sys.stdout)
            time.sleep(0.1)
            return

        q = self.responder.response_queue
        try:
            session = TcpSession(self.dispatcher, q, connection, address, use_ssl=self.use_ssl, ssl_certfile=self.ssl_certfile, ssl_keyfile=self.ssl_keyfile)
        except BaseException, e:
            error = str(e)
            print_log("cannot start TCP session", error, address)
            connection.close()
            time.sleep(0.1)
            return
             
        self.input_list.append(connection)
        self.session_list[connection] = session


    def on_close(self):
        #print "closing session", self.session.address
        self.session.stop()
        self.session_list.pop(self.s)
        self.input_list.remove(self.s)
        self.s.close()


    def on_recv(self):
        self.session.message += self.data
        while self.parse():
            pass


    def parse(self):
        session = self.session
        message = session.message
        session.time = time.time()

        raw_buffer = message.find('\n')
        if raw_buffer == -1:
            return False

        raw_command = message[0:raw_buffer].strip()
        session.message = message[raw_buffer + 1:]

        if raw_command == 'quit':
            session.stop()
            return False

        try:
            command = json.loads(raw_command)
        except:
            #self.dispatcher.push_response(session, {"error": "bad JSON", "request": raw_command})
            print "bad json", raw_command
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

        return True


    def run(self):

        self.responder = TcpClientResponder(self.shared)
        self.responder.start()

        print_log( ("SSL" if self.use_ssl else "TCP") + " server started on port %d"%self.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(5)
        self.input_list.append(sock)

        while not self.shared.stopped():
            time.sleep(self.delay)
            try:
                inputready, outputready, exceptready = select.select(self.input_list, [], [])
            except socket.error:
                traceback.print_exc(file=sys.stdout)
                time.sleep(1)
                continue

            for self.s in inputready:
                if self.s == sock:
                    self.on_accept()
                    break
                
                self.session = self.session_list[self.s]
                try:
                    self.data = self.session._connection.recv(self.buffer_size)
                except:
                    self.data = ''

                if len(self.data) == 0:
                    self.on_close()
                else:
                    self.on_recv()


