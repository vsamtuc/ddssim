#
# Simulation components
# 


class Channel:
	def __init__(self, src, dst):
		self.src = src  # source node
		self.dst = dst  # dest node
		self.msg = 0    # no of msg sent
		self.bytes = 0  # no of msg recv

	def send(self, msgtype, msg):
		self.msg += 1
		self.bytes += msgtype.size_in_bytes(msg)
		self.dest.recv(self, msgtype, msg)



class Host:
	def __init__(self, nid):
		self.nid = nid   # node id
		self.peers = set()
		self.recv_chan = {}
		self.send_chan = {}
		self.handlers = {}

	def connect(self, peer, recv_chan, send_chan):
		self.peers.add(peer)
		self.recv_chan[peer] = recv_chan
		self.send_chan[peer] = send_chan


	def send(self, peer, msgtype, msg):
		self.send_chan[peer].send(msgtype, msg)

	def add_handler(self, msgtype, handler_func):
		self.handlers[msgtype] = handler_func

	def recv(self, channel, msgtype, msg):
		self.handlers[msgtype](channel, msgtype, msg)



class CoordNetwork:
	def __init__(self, k, node_type=Host, coord_type=Host, channel_type=Channel):
		self.k = k    # number of nodes

		self.node_type = node_type
		self.coord_type = coord_type
		self.channel_type = channel_type

		self.nodes = [node_type(i) for i in range(k)]
		self.coord = coord_type(None)

		# set up channels
		for node in self.nodes:
			self.link(node, self.coord)

	def link(self, node, coord):
		"Create the connection between a node and a coordinator"
		uplink = self.channel_type(node, coord)
		dnlink = self.channel_type(coord, node)
		node.connect(coord, dnlink, uplink)
		coord.connect(node, uplink, dnlink)



