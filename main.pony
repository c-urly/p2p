use "time"
use "collections"
use "random"
use "math"

actor Main
  let _env: Env
  var numNodes: U64
  var numRequests: U64
  var numKeys: U64
  var nodes_map: Map[U64, Node tag] = Map[U64, Node tag]
  var _rand: Rand
  var total_hops: U64 = 0
  var total_requests: U64 = 0
  let timers : Timers = Timers
  let node_ids: Array[U64]
  let temp_array: Array[U64]
  let initial_data: Map[U64, String]
  var all_keys: MinHeap[U64]
  var stabilized_nodes:Array[U64]

  new create(env: Env) =>
    _env = env
    _rand = Rand(Time.now()._2.u64())
    numNodes = 0
    numRequests = 0
    numKeys = 0
    node_ids = Array[U64]
    temp_array = Array[U64]
    initial_data = Map[U64, String]
    all_keys = MinHeap[U64](10)
    stabilized_nodes = Array[U64]
    if env.args.size() != 3 then
      env.out.print("Usage: p2p <numNodes> <numRequests>")
      return
    end

    try
      numNodes = env.args(1)?.u64()?
      numRequests = env.args(2)?.u64()?
      numKeys = 3 * numNodes
      all_keys = MinHeap[U64](numKeys.usize())
      env.out.print("numNodes: " + numNodes.string())
      env.out.print("numRequests: " + numRequests.string())
      env.out.print("numKeys: " + numKeys.string())


    try
      generate_nodes_with_keys(numNodes, numKeys)?
      // simulate_requests(numRequests)
    else
      env.out.print("Error getting keys")
    end
      let notify = Notify(this, _env)  // Pass 'Main' ref to Notify
      let timer = Timer(consume notify, 5_000_000_000, 0)
      // timers(consume timer)
    else
      env.out.print("Error: Unable to parse arguments.")
    end


  fun ref generate_nodes_with_keys(num_nodes: U64, num_keys: U64)? =>
      let m: USize = 32  // Setting m to 10, or calculate based on num_nodes if needed
      let id_space: U64 = (1 << m.u64()) - 1  // 2^m - 1 for the ID space

      let keys_per_node = num_keys / num_nodes
      
      var nodes_list: Array[(U64, Node tag)] = Array[(U64, Node tag)](num_nodes.usize())
      var max_id: U64 = 0
      var node_ids_set: Set[U64] = Set[U64]()  // Set to keep track of unique node IDs

      // First, generate nodes and store them in nodes_list
      for i in Range[U64](0, num_nodes) do
        var node_id: U64 = _rand.int_unbiased(id_space)
        
        // Ensure node_id is unique
        while node_ids_set.contains(node_id) do
          node_id = _rand.int_unbiased(id_space)
        end
        
        let node: Node tag = Node(_env, this, node_id, m)
        nodes_list.push((node_id, node))
        max_id = max_id.max(node_id)
        _env.out.print("[Rishi]Generated node with ID: " + node_id.string())
        nodes_map.update(node_id, node)  // Store in nodes_map
        node_ids_set.add(node_id)  // Add to set to ensure uniqueness
      end

      // Select a random bootstrap node from nodes_list
      let bootstrap_index = _rand.int_unbiased(num_nodes)
      let bootstrap_node: Node = nodes_list(bootstrap_index.usize())?._2
      let bootstrap_node_id: U64 = nodes_list(bootstrap_index.usize())?._1
      _env.out.print("[Rishi]Selected bootstrap node with ID: " + bootstrap_node_id.string())

      // Now have each node (excluding the bootstrap) join the network via bootstrap node
      for i in Range[U64](0, nodes_list.size().u64()) do
        let node_id: U64 = nodes_list(i.usize())?._1
        let node: Node = nodes_list(i.usize())?._2
        if node_id != bootstrap_node_id then
          bootstrap_node.join(node)
          _env.out.print("Node with ID " + node_id.string() + " joined the network via bootstrap node " + bootstrap_node_id.string())
        end
      end

      _env.out.print("[Rishi]Max Id: " + max_id.string())

      var all_keys_set: Set[U64] = Set[U64]()  // Set to track unique keys

      for i in Range[U64](0, num_keys) do
        var key: U64 = _rand.int_unbiased(max_id)
        
        // Ensure key is unique
        while all_keys_set.contains(key) do
          key = _rand.int_unbiased(max_id)
        end
        
        all_keys.push(key)
        temp_array.push(key)
        initial_data(key) = "File"+key.string()
        _env.out.print("[Rishi] Key: "+key.string()+ " " + "File-"+key.string())
        all_keys_set.add(key)
      end

      for key in nodes_map.keys() do
        node_ids.push(key)
      end

      Sort[Array[U64], U64](node_ids)

      // Assign keys to nodes based on `key <= node_id` rule
      for i in Range[U64](0, node_ids.size().u64()) do
        try
          let current_node_id = node_ids(i.usize())?

          // Assign all keys less than or equal to current_node_id
          while (all_keys.size() > 0) and (all_keys.peek()? <= current_node_id) do
            let k: U64 = all_keys.peek()?
            nodes_map(current_node_id)?.store_key(k, initial_data(k)?)
            _env.out.print("Key : " + k.string() + " in Node_id: " + current_node_id.string())
            all_keys.pop()?
          end
        else
          _env.out.print("Key or value not found!")
        end
      end

      // Fallback assignment for remaining keys
      while all_keys.size() > 0 do
        let k: U64 = all_keys.pop()?
        let first_node_id = node_ids(0)?
        nodes_map(first_node_id)?.store_key(k, initial_data(k)?)
        _env.out.print("Fallback assignment - Key: " + k.string() + " in Node_id: " + first_node_id.string())
      end



 

  be lookup_key() =>
    // _env.out.print("Simulating " + num_requests.string() + " requests per node.")
    try 
      for node_id in nodes_map.keys() do
        let node: Node = nodes_map(node_id)?

        for _ in Range[U64](0, numRequests) do
          let random_key:U64 = temp_array((_rand.u64() % temp_array.size().u64()).usize())?
          _env.out.print("Node " + node_id.string() + " is looking up key " + random_key.string())
          node.lookup_key(random_key)
        end
      end
    else
      _env.out.print("[Lookup]Key index Out of bound")
    end

  // be lookup_key() =>

  //     try
  //       let random_key:U64 = temp_array((_rand.u64() % temp_array.size().u64()).usize())?
  //       let random_id:U64 = node_ids((_rand.u64() % node_ids.size().u64()).usize())?
  //       _env.out.print("[Rishi]Node " + random_id.string() + " is looking up key " + random_key.string())
  
  //         nodes_map(random_id)?.lookup_key(random_key)
  //     else
  //       _env.out.print("Value not available")
  //     end

  be receive_hop_count(node_id:U64, hops: U64) =>
    total_hops = total_hops + hops
    total_requests = total_requests + 1
    _env.out.print("Node id: "+ node_id.string() +",Hop Count: " + hops.string())

    if total_requests >= (numNodes * numRequests) then
      for node in nodes_map.values() do
        node.stop()
      end
      calculate_average_hops()
    end

  fun ref calculate_average_hops() =>
    if total_requests > 0 then
      let average_hops:F64 = (total_hops.f64() / total_requests.f64()).f64()
      _env.out.print("Average number of hops per lookup: " + average_hops.string())
    else
      _env.out.print("No requests completed.")
    end

  be node_stabilized(node_id: U64) =>
    stabilized_nodes.push(node_id)
    // _env.out.print("Node " + node_id.string() + " reported stabilization.")
    
    if stabilized_nodes.size().u64() == numNodes then
      _env.out.print("Chord network has fully stabilized.")
      // Perform any further actions, such as starting lookups
      lookup_key()
    end


class Notify is TimerNotify
  let _main: Main
  let _env: Env

  new iso create(main: Main, env: Env) =>
    _main = main
    _env = env

  fun ref apply(timer: Timer, count: U64): Bool =>
    _env.out.print("Notify triggered.")
    // _main.lookup_key()
    true