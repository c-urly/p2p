use "time"
use "collections"
use "random"

actor Node
  let _env: Env
  var _id: U64
  var _successor: Node
  var _predecessor: Node
  var _successor_id: U64
  var _predecessor_id: U64
  var _finger_table: Array[(U64,Node | None)]
  var _previous_finger_table: Array[(U64,Node | None)]
  var _next_finger: USize
  var _m: USize
  var _timer: Timer tag
  var _stabilize_timer: Timer tag
  var _predecessor_check_timer: Timer tag
  var _data: Map[U64, String] iso
  let _main: Main
  let timers: Timers
  let _rand: Rand
  var successor_stable_rounds: U64 = 0
  var predecessor_stable_rounds: U64 = 0
  var finger_table_stable_rounds: U64 = 0
  var _stabilized: Bool


  new create(env: Env, main: Main, id: U64, m: USize) =>
    timers = Timers
    _env = env
    _id = id
    _m = m
    _previous_finger_table = Array[(U64, (Node | None))](_m)
    _finger_table = Array[(U64, (Node | None))](_m)
    for i in Range[USize](0, _m) do
      _finger_table.push((0, None))
      _previous_finger_table.push((0,None))
    end
    _next_finger = 0
    _predecessor = this
    _successor = this
    _data = Map[U64,String]
    _successor_id = id
    _predecessor_id = id
    _main = main
    _stabilized = false
    _rand = Rand(Time.now()._2.u64())

    _env.out.print("Node " + _id.string() + " created with " + m.string() + " bit hash space and initial keys.")


    let stabilize_interval:U64 = 1_000_000_000
    let stabilize_notify = ChordTimerNotify(_env, this, _id, "stabilize")
    let stabilize_timer' = Timer(consume stabilize_notify, stabilize_interval, stabilize_interval)
    _stabilize_timer = stabilize_timer'
    timers(consume stabilize_timer')

    let fix_fingers_interval:U64 = 1_000_000
    let fix_fingers_notify = ChordTimerNotify(_env, this, _id, "fix_fingers")
    let fix_fingers_timer = Timer(consume fix_fingers_notify, fix_fingers_interval, fix_fingers_interval)
    _timer = fix_fingers_timer
    timers(consume fix_fingers_timer)

    let check_predecessor_interval:U64 = 1_000_000_000
    let check_predecessor_notify = ChordTimerNotify(_env, this, _id, "check_predecessor")
    let check_predecessor_timer = Timer(consume check_predecessor_notify, check_predecessor_interval, check_predecessor_interval)
    _predecessor_check_timer = check_predecessor_timer
    timers(consume check_predecessor_timer)


  be join(node: Node) =>
    _predecessor = this
    _predecessor_id = _id
    find_successor(_id, node, "find_successor")


  be receive_successor(successor: Node, successor_id: U64) =>
    _env.out.print("Successor for node " + _id.string() + " updated to " + successor_id.string())
    _successor = successor
    _successor_id = successor_id



be find_successor(id: U64, requestor: Node, purpose: String = "find_successor", hop_count: U64 = 0, finger_index: USize = USize.max_value()) =>
 

  if in_range(id, _id, _successor_id) then
   
    match purpose
    | "find_successor" =>
      requestor.receive_successor(_successor, _successor_id)
    | "update_finger" =>
      if finger_index != USize.max_value() then
        // _env.out.print("Updating finger table for finger " + finger_index.string())
        requestor.update_finger(finger_index, _successor, _successor_id)
      else
        _env.out.print("Finger index not specified for finger table update.")
      end
    else
      _env.out.print("Unknown purpose in find_successor.")
    end
  else
    let closest_node = closest_preceding_node(id)

    match purpose
    | "lookup_key" =>
        closest_node.find_successor(id, requestor, purpose, hop_count + 1)
    | "update_finger" =>
        closest_node.find_successor(id, requestor, purpose, 0, finger_index)
    | "find_successor" =>
        closest_node.find_successor(id, requestor, purpose)
    end  
  end

  be perform_key_lookup(key: U64, requestor: Node, hop_count: U64 = 0) =>
    _env.out.print("[Rishi]Lookup for key " + key.string() + " in " + _id.string() + " at hop " + hop_count.string())

    if in_range(key, _predecessor_id, _id) then
      try

        let value = _data(key)?
        requestor.receive_lookup_result(key, value, hop_count)
      else
        requestor.receive_lookup_result(key, "None", hop_count)
      end
    else

      let closest_node = closest_preceding_node(key)
      if closest_node is this then
        _successor.perform_key_lookup(key, requestor, hop_count + 1)
      else
        closest_node.perform_key_lookup(key, requestor, hop_count + 1)
      end
    end


  be lookup_key(key: U64) =>
    perform_key_lookup(key, this, 0)

  be receive_lookup_result(key: U64, value: String, hops: U64) =>
    if value is "None" then
      _env.out.print("[Rishi]Lookup result for key " + key.string() + " not found after " + hops.string() + " hops.")
    else
      _env.out.print("[Rishi]Lookup result for key " + key.string() + " found in " + hops.string() + " hops.")
      print_finger_table()
      _main.receive_hop_count(hops)
    end


  be print_finger_table() =>
    _env.out.print("Finger table for node " + _id.string() + ":")
    for i in Range[USize](0, _m) do
      try
        let finger_entry = _finger_table(i)?
        let finger_id: U64 = finger_entry._1
        match finger_entry._2
        | let node: Node =>
          _env.out.print("Finger " + i.string() + ", Node ID = " + finger_id.string())
        | None =>
          _env.out.print("Finger " + i.string() + ": None node ID = " + finger_id.string())
        end
      else
        _env.out.print("Error accessing finger table at index " + i.string())
      end
    end




  be fix_fingers() =>
    _env.out.print("Calling Fix Fingers")

    if _next_finger >= _m then
      _next_finger = 0
      try
        check_finger_table_stabilization()?
      else
        _env.out.print("Finger table Index Out of bound")
      end
    end

    let target_key: U64 = (_id + (1 << _next_finger).u64()) % (1 << _m).u64()
    find_successor(target_key, this, "update_finger", 0, _next_finger)
    
    _next_finger = _next_finger + 1


  be update_finger(finger_index: USize, node: Node, id: U64) =>
    // _env.out.print("Update finger")
      try
        // _env.out.print("[Rishi] updating id: " + id.string())
        _finger_table(finger_index)? = (id, node)
      else
        _env.out.print("Index not found!!")
      end
      print_finger_table()
      

  fun ref closest_preceding_node(id: U64): Node =>
    var i: I64 = _finger_table.size().i64() - 1

    while i >= 0 do
      try
        let finger: (U64, (Node | None)) = _finger_table(i.usize())?

        // Use match to handle cases where Node is None
        match finger._2
        | let node: Node =>
          // Check if finger ID is within the range (_id, id)
          if in_range(finger._1, _id, id) then
            return node
          end
        | None =>
          // If Node is None, skip to the next finger entry
          i = i - 1
          continue
        end
      else
        _env.out.print("Error accessing keys or finger table. Continuing...")
      end

      i = i - 1
    end

    // If no suitable preceding node is found, return `this` node
    this


  be lookupkey()=>
    None

  be store_key(key: U64, value: String) =>
    _data(key) = value
    _env.out.print("[Rishi]Stored key " + key.string() + " with value '" + value.string() + "' at node " + _id.string())

  fun in_range(id: U64, id_start: U64, id_end: U64): Bool =>
    if id_start < id_end then
      (id > id_start )and (id <= id_end)
    else
      (id > id_start) or (id <= id_end)
    end

  be check_predecessor() =>
    // _env.out.print("Checking if predecessor is alive.")
    _env.out.print("Successor of node :" + _id.string() + " is " + _successor_id.string())
    _env.out.print("Predecessor of node :" + _id.string() + " is " + _predecessor_id.string())
    _predecessor.alive(this)
  

  be alive(response_to: Node) =>
    response_to.receive_alive_signal(this, _id)

  be receive_alive_signal(caller: Node, caller_id: U64) =>
    if _predecessor_id == caller_id then
      // _env.out.print("Confirmed that predecessor " + caller_id.string() + " is alive for node: " + _id.string())
      None
    else
      _env.out.print("Received alive signal from unknown node.")
    end

  fun ref check_stabilization() =>
    if (((successor_stable_rounds >= 2 )and (predecessor_stable_rounds >= 2)) and (finger_table_stable_rounds >= 1) ) and (not _stabilized) then
      _stabilized = true
       _main.node_stabilized(_id)
    end

  fun ref check_finger_table_stabilization()? =>
    var is_stabilized = true

    for i in Range[USize](0, _m) do
      let current_entry = _finger_table(i)?
      let previous_entry = _previous_finger_table(i)?

      if (current_entry._1 != previous_entry._1) then
        is_stabilized = false
        break
      end
    end

    if is_stabilized then
      finger_table_stable_rounds = finger_table_stable_rounds + 1
      check_stabilization()
    else
      finger_table_stable_rounds = 0
      for i in Range[USize](0, _m) do
        _previous_finger_table(i)? = _finger_table(i)?
      end
    end


  be notify(caller: Node, caller_id: U64) =>

      if in_range(caller_id, _predecessor_id, _id) then
        _predecessor = caller
        _predecessor_id = caller_id
        // _env.out.print("Predecessor updated to " + caller_id.string())
      else
        predecessor_stable_rounds = predecessor_stable_rounds +  1
        // _env.out.print("Predecessor remains unchanged.")
      end

      check_stabilization()



  be stabilize() =>
    // _env.out.print("Stabilizing node " + _id.string())
    _successor.request_predecessor(this)


  be receive_predecessor(pred: Node, pred_id: U64) =>

      if in_range(pred_id, _id, _successor_id) then
        _successor = pred
        _successor_id = pred_id
        // _env.out.print("Node id: " + _id.string() + " Updated successor to " + pred_id.string())
      else
        successor_stable_rounds = successor_stable_rounds + 1
        // _env.out.print("Node id: " + _id.string() + " No update to successor: " + pred_id.string() )
      end

      _successor.notify(this, _id)
      check_stabilization()
  


  be request_predecessor(requestor: Node) =>
    requestor.receive_predecessor(_predecessor, _predecessor_id)



 
    
  be stop() =>
    timers.cancel(_timer)
    timers.cancel(_stabilize_timer)
    timers.cancel(_predecessor_check_timer)
