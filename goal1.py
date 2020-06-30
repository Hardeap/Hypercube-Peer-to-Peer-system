
import Queue
import argparse
import logging
import thread
import threading
import time

#ndn
class P2PNode(threading.Thread):
    def __init__(self, n, node_id):
        threading.Thread.__init__(self)  

        self._n = n
        self._peers = []  
        self._proceed = True  

        self.id = node_id
        self.message_queue = Queue.Queue() 

    def _find_peers(self):
        while self._need_peers():
            for i in xrange(self._n):
                peer = nodes[self.id ^ (1 << i)]
                if peer is not None and peer not in self._peers:
                    self._peers.append(peer)
            if self._need_peers():
                time.sleep(1) 

    def _get_message(self):
        message = None

        try:
            message = self.message_queue.get(timeout=1) 
            logging.debug("{s} got message: {m}".format(s=self, m=message))
        except Queue.Empty:
            pass

        return message

    def _need_peers(self):
        return len(self._peers) < self._n

    def _process_message(self, message):
        message_text = message[0]
        if message_text == "die":
            self._proceed = False
        elif message_text == "report":
            logging.info("{} peers: {}".format(self, self._peers))
        else:
            logging.warning("in {}: unknown message: {}".format(self, message))

    def run(self):
        self._find_peers()  

        while self._proceed:
            message = self._get_message()
            while self._proceed and message:
                try:
                    self._process_message(message)
                    self.message_queue.task_done()
                except Exception as e:
                    error_message_template = "In {s}: {t}: {e}; invalid message: {m}"
                    logging.error(error_message_template.format(e=e, m=message, s=self, t=e.__class__.__name__))
                message = self._get_message()

def init_nodes(n):
    global nodes

    nodes = [None] * (2**n)  

    try:
        for node_id in xrange(2**n):
            node = P2PNode(n, node_id)
            node.setName("id={i:0{n}b}".format(i=node_id, n=n))
            node.start()
            nodes[node_id] = node
        logging.info("{} nodes started".format(len(nodes)))
    except thread.error:
        logging.critical("can't create more threads; current amount of nodes is {}".format(len(filter(None, nodes))))
        kill_all_nodes()


def kill_all_nodes():
    for node in filter(None, nodes):
        node.message_queue.put(("die",))


def print_report():

    for node in filter(None, nodes):
        node.message_queue.put(("report",))


def wait_stop():

    try:
        while True:
            pass
    except KeyboardInterrupt:
        logging.info("Requesting reports, print Ctrl+C again to terminate...")
        print_report()
        try:
            while True:
                pass
        except KeyboardInterrupt:
            logging.info("Terminating the nodes...")
            kill_all_nodes()

if __name__ == "__main__":

    log_level_choices = ['DEBUG', 'INFO']
    args_parser = argparse.ArgumentParser(description="Hypercube P2P system")
    args_parser.add_argument("-l", "--log-level", choices=log_level_choices, type=str.upper, help="set logging level")
    args_parser.add_argument("-n", default=2, metavar="INTEGER", type=int, help="set number of hypercube dimensions")
    args = args_parser.parse_args()
    if args.n < 2:
        args_parser.error("argument -n: invalid value: {}; must be 2 or more".format(args.n))

    if args.log_level:
        logging.basicConfig(level=args.log_level)

    init_nodes(args.n)
    wait_stop()
