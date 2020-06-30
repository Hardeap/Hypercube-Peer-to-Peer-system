import Queue
import argparse
import logging
import random
import string
import thread
import threading
import time


retrieve_failure_count = 0
retrieve_failure_lock = threading.Lock()
store_failure_count = 0
store_failure_lock = threading.Lock()


class Evil(threading.Thread):

    def __init__(self, q=None, r=None):

        threading.Thread.__init__(self)

        self._q = q if q is not None else 1
        self._r = r if r is not None else 10
        self._proceed = True

    def run(self):

        nodes_killed = 0
        while self._proceed and nodes_killed < self._q:
            time.sleep(self._r)
            running_nodes = [x for x in threading.enumerate() if isinstance(x, P2PNode)]
            if running_nodes:
                victim = random.choice(running_nodes)
                victim.message_queue.put({"cmd": "fail"})
                logging.info("The evil ordered {} to die!".format(victim))
                nodes_killed += 1

    def stop(self):

        self._proceed = False


class P2PNode(threading.Thread):

    tracks = {"get": [], "put": []}

    def __init__(self, node_id, n, m=None, p=None, nodelay=False):

        threading.Thread.__init__(self)

        self._n = n
        self._m = m
        self._p = p
        self._dead = False
        self._files = []
        self._fkeys = set()
        self._peers = []
        self._proceed = True
        self._files_produced = 0
        self._files_retrieved = 0
        self._nodelay = nodelay

        self.id = node_id
        self.message_queue = Queue.Queue()

    @staticmethod
    def _bit_distance(a, b):
        n = a ^ b
        count = 0
        while n:
            count += n & 1
            n >>= 1
        return count

    def _bounce_get(self, message):

        message["dst"] = message["track"][0]
        message["file"] = random.choice(self._files)
        self._transmit(message)
        log_message_template = "{s} sent file '{f}' (key: {k:0{n}b} or {k}) to node {d}"
        log_message = log_message_template.format(d=nodes[message["dst"]],
                                                  f=message["file"],
                                                  k=self.id,
                                                  n=self._n,
                                                  s=self)
        logging.debug(log_message)

    def _calculate_file_hash(self, f):

        return sum(ord(c) for c in f) % (2**self._n)

    def _do_other_things(self):
        if not self._nodelay:
            time.sleep(random.randint(1, 10))

    def _find_peers(self):

        while self._need_peers():
            for i in xrange(self._n):
                peer = nodes[self.id ^ (1 << i)]
                if peer is not None and peer not in self._peers:
                    self._peers.append(peer)
            if self._need_peers():
                time.sleep(1)

    def _generate_file(self):
        if not self._nodelay:
            time.sleep(random.randint(1, 10))
        f = ''.join(random.choice(string.ascii_uppercase+string.digits) for _ in range(4))
        k = self._calculate_file_hash(f)
        logging.debug("{s} generated file '{f}' (key: {k:0{n}b} or {k})".format(f=f, k=k, n=self._n, s=self))
        return f

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

    def _print_report(self):

        logging.info("{} files: {}".format(self, self._files))
        logging.info("{} peers: {}".format(self, self._peers))
        logging.info("{} keys of generated files: {}".format(self, self._fkeys))

    def _process_message(self, message):

        global store_failure_count
        global retrieve_failure_count

        if self._dead:
            if message["cmd"] == "die":
                self._proceed = False
            elif message["cmd"] == "get":
                with retrieve_failure_lock:
                    retrieve_failure_count += 1
                log_message_template = "failed to retrieve file with key: {k:0{n}b} or {k}: node {s} is dead"
                logging.debug(log_message_template.format(k=message["dst"], n=self._n, s=self))
            elif message["cmd"] == "put":
                with store_failure_lock:
                    store_failure_count += 1
                log_message_template = "failed to store file '{f}' (key: {k:0{n}b} or {k}): node {s} is dead"
                logging.debug(log_message_template.format(f=message["file"], k=message["dst"], n=self._n, s=self))
            return

        if "dst" in message:
            if message["dst"] != self.id:
                self._transmit(message)
                return

        if message["cmd"] == "die":
            self._proceed = False
        if message["cmd"] == "fail":
            self._dead = True
        elif message["cmd"] == "get":
            if "file" not in message:
                self._bounce_get(message)
            else:
                self._retrieve_file(f=message["file"], track=message["track"])
        elif message["cmd"] == "put":
            self._store_file(k=message["dst"], f=message["file"], track=message["track"])
        elif message["cmd"] == "report":
            self._print_report()
        else:
            logging.warning("in {}: unknown message: {}".format(self, message))

    def _retrieve_file(self, f=None, k=None, track=None):

        if f is None and k is not None:
            # Start retrieving file by key 'k'.
            if k == self.id:
                f = random.choice(self._files)
                self._store_track("get", track)
                log_message_template = "{s} retrived file '{f}' (key: {k:0{n}b} or {k}) from itself"
                logging.debug(log_message_template.format(f=f, k=k, n=self._n, s=self))
            else:
                self._transmit(message={"cmd": "get", "dst": k})
        elif k is None and f is not None:
            k = self._calculate_file_hash(f)
            self._store_track("get", track)
            log_message_template = "{s} retrieved file '{f}' (key: {k:0{n}b} or {k}) from node {r}"
            logging.debug(log_message_template.format(f=f, k=k, n=self._n, r=nodes[k], s=self))
        else:
            logging.error("in {}: both f and k are None".format(self))

    def _store_file(self, f, k=None, track=None):

        if k is None:
            k = self._calculate_file_hash(f)

        if k == self.id:
            self._files.append(f)
            self._store_track("put", track)
            logging.debug("file '{f}' (key: {k:0{n}b} or {k}) stored at {s}".format(f=f, k=k, n=self._n, s=self))
        else:
            self._transmit(message={"cmd": "put", "dst": k, "file": f})

    def _store_track(self, cmd, track=None):

        if track is None:
            track = []
        track.append(self.id)
        P2PNode.tracks[cmd].append(track)

    def _transmit(self, message):

        if "track" not in message:
            message["track"] = [self.id]
        else:
            message["track"].append(self.id)

        best_match = None
        best_match_distance = self._n + 1
        for peer in self._peers:
            bit_distance = self._bit_distance(message["dst"], peer.id)
            if bit_distance < best_match_distance:
                best_match = peer
                best_match_distance = bit_distance

        best_match.message_queue.put(message)

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
            if self._dead:
                continue

            if self._proceed:
                if self._m is not None:
                    if self._files_produced < self._m:
                        f = self._generate_file()
                        self._files_produced += 1
                        self._store_file(f)
                        self._fkeys.add(self._calculate_file_hash(f))
                        if self._proceed:
                            self._do_other_things()
                    else:
                        if self._files_retrieved < self._p:
                            self._do_other_things()
                            key_of_file_to_retrieve = random.choice(tuple(self._fkeys))
                            self._retrieve_file(k=key_of_file_to_retrieve)
                            self._files_retrieved += 1
                            if self._proceed:
                                self._do_other_things()


def init_evil(q=None, r=None):

    global evil

    if q is not None or r is not None:
        evil = Evil(q, r)
        evil.setName("Evil")
        evil.start()


def init_nodes(n, m=None, p=None, nodelay=False):

    global nodes

    nodes = [None] * (2**n)

    try:
        for node_id in xrange(2**n):
            node = P2PNode(node_id=node_id, n=n, m=m, p=p, nodelay=nodelay)
            node.setName("id={i:0{n}b}".format(i=node_id, n=n))
            node.start()
            nodes[node_id] = node
        logging.info("{} nodes started".format(len(nodes)))
    except thread.error:
        logging.critical("can't create more threads; current amount of nodes is {}".format(len(filter(None, nodes))))
        kill_all_nodes()


def kill_all_nodes():
    for node in filter(None, nodes):
        node.message_queue.put({"cmd": "die"})
    for node in filter(None, nodes):
        node.join()


def print_hops_average(m=None, p=None):

    if m is not None:
        hops = [len(track) - 1 for track in P2PNode.tracks["put"]]
        logging.info("average number of hops to store file: {}".format(float(sum(hops)) / max(len(hops), 1)))
    if p is not None:
        hops = [len(track) - 1 for track in P2PNode.tracks["get"]]
        logging.info("average number of hops to retrieve file: {}".format(float(sum(hops)) / max(len(hops), 1)))


def print_report():
    for node in filter(None, nodes):
        node.message_queue.put({"cmd": "report"})


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
            evil.stop()


if __name__ == "__main__":

    log_level_choices = ['DEBUG', 'INFO']
    args_parser = argparse.ArgumentParser(description="Hypercube P2P system")
    args_parser.add_argument("-l", "--log-level", choices=log_level_choices, type=str.upper, help="set logging level")
    args_parser.add_argument("-n", default=2, metavar="INTEGER", type=int, help="set number of hypercube dimensions")
    args_parser.add_argument("-m", metavar="INTEGER", type=int, help="set number of files to generate by each node")
    args_parser.add_argument("-p", metavar="INTEGER", type=int, help="set number of files to retrieve by each node")
    args_parser.add_argument("-q", metavar="INTEGER", type=int, help="set the number of nodes attacked")
    args_parser.add_argument("-r", metavar="INTEGER", type=int, help="set the evil mastermind time (in seconds)")
    args_parser.add_argument("-y", "--no-delay", action="store_true", help="turn off delays imitating user actions")
    args = args_parser.parse_args()
    if args.n < 2:
        args_parser.error("argument -n: invalid value: {}; must be 2 or more".format(args.n))
    if args.m is not None:
        if args.m <= 0:
            args_parser.error("argument -m: invalid value: {}; must be positive".format(args.m))
    if args.p is not None:
        if args.m is None:
            args_parser.error("argument -p cannot be used without -m")
        if args.p <= 0:
            args_parser.error("argument -p: invalid value: {}; must be positive".format(args.p))
        if args.p > args.m:
            args_parser.error("argument -p: invalid value: {}; p cannot be greater than m".format(args.p))
    if args.q is not None:
        if args.q <= 0:
            args_parser.error("argument -q: invalid value: {}; must be positive".format(args.q))
    if args.r is not None:
        if args.r <= 0:
            args_parser.error("argument -r: invalid value: {}; must be positive".format(args.r))

    if args.log_level:
        logging.basicConfig(level=args.log_level)

    init_nodes(args.n, args.m, args.p, args.no_delay)
    init_evil(args.q, args.r)
    wait_stop()
    print_hops_average(args.m, args.p)

    if store_failure_count > 0:
        logging.warning("Failed to store {} of {} files".format(store_failure_count, (2**args.n)*args.m))
    if retrieve_failure_count > 0:
        logging.warning("Failed to retrieve {} of {} files".format(retrieve_failure_count, (2**args.n)*args.p))
