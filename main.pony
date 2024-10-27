use "time"
use "collections"
use "random"

actor Main
  let _env: Env
  var numNodes: U64
  var numRequests: U64
  var numKeys: U64
  var nodes_map: Map[U64, Node tag] = Map[U64, Node tag]
  var _rand: Rand
  var total_hops: U64 = 0
  var total_requests: U64 = 0
  var all_keys: Array[U64] = Array[U64]

  new create(env: Env) =>
    _env = env
    _rand = Rand(Time.now()._2.u64())
    numNodes = 0
    numRequests = 0
    numKeys = 0

    if env.args.size() != 3 then
      env.out.print("Usage: p2p <numNodes> <numRequests>")
      return
    end

    try
      numNodes = env.args(1)?.u64()?
      numRequests = env.args(2)?.u64()?
      numKeys = 3 * numNodes

      env.out.print("numNodes: " + numNodes.string())
      env.out.print("numRequests: " + numRequests.string())
      env.out.print("numKeys: " + numKeys.string())


      generate_nodes_with_keys(numNodes, numKeys)

    try
      simulate_requests(numRequests)?
    else
      env.out.print("Error getting keys")
    end
    else
      env.out.print("Error: Unable to parse arguments.")
    end


  fun ref generate_nodes_with_keys(num_nodes: U64, num_keys: U64) =>
    var bootstrap_node: (Node | None) = None
    var bootstrap_node_id: U64 = 0

    let m: USize = ((num_nodes * 3).log2().ceil().usize())
    let max_id: U64 = (1 << m) - 1  // Calculate 2^m - 1 for ID and key space

    let keys_per_node = num_keys / num_nodes

    for i in Range[U64](0, num_nodes) do
      // Generate a node ID within the range [0, 2^m - 1]
      let node_id: U64 = _rand.u64() % (max_id + 1)
      var initial_data: Map[U64, String] iso = Map[U64, String]

      // Generate unique keys for this node, also constrained to [0, 2^m - 1]
      for j in Range[U64](0, keys_per_node) do
        let key: U64 = _rand.u64() % (max_id + 1)
        let value: String = "Value" + key.string()
        initial_data(key) = value
        all_keys.push(key)
      end

      // Create Node
      var node: Node tag = Node(_env, this, node_id, m, consume initial_data)

      if bootstrap_node is None then
        bootstrap_node = node
        bootstrap_node_id = node_id
        _env.out.print("Bootstrap node created with ID: " + node_id.string())
      else
        node.join(bootstrap_node)
        _env.out.print("Node with ID " + node_id.string() + " joined the network via bootstrap node " + bootstrap_node_id.string())
      end

      nodes_map.update(node_id, node)
    end


  fun ref simulate_requests(num_requests: U64)? =>
    _env.out.print("Simulating " + num_requests.string() + " requests per node.")

    for node_id in nodes_map.keys() do
      let node = nodes_map(node_id)?

      for _ in Range[U64](0, num_requests) do
        let random_key:U64 = all_keys((_rand.u64() % all_keys.size().u64()).usize())?
        _env.out.print("Node " + node_id.string() + " is looking up key " + random_key.string())
        node.lookup_key(random_key)
      end
    end


  be receive_hop_count(hops: U64) =>
    total_hops = total_hops + hops
    total_requests = total_requests + 1


    if total_requests == (numNodes * numRequests) then
      calculate_average_hops()
    end

  fun ref calculate_average_hops() =>
    if total_requests > 0 then
      let average_hops = (total_hops / total_requests)
      _env.out.print("Average number of hops per lookup: " + average_hops.string())
    else
      _env.out.print("No requests completed.")
    end
