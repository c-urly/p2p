use "time"
use "collections"

actor Node
  let _env: Env
  var _id: U64
  var _successor: (Node | None)
  var _predecessor: (Node | None)
  var _successor_id: U64
  var _predecessor_id: U64
  var _finger_table: Map[U64,(Node|None)]
  var _next_finger: USize
  var _m: USize
  var _timer: Timer tag
  var _stabilize_timer: Timer tag
  var _predecessor_check_timer: Timer tag
  var _data: Map[U64, String] iso
  let _main: Main
  let timers: Timers

  new create(env: Env, main: Main,  id: U64, m: USize, initial_data: Map[U64, String] iso, timers': Timers) =>
    timers = timers'
    _env = env
    _id = id
    _m = m
    _finger_table = Map[U64,(Node|None)]
    _next_finger = 0
    _predecessor = None
    _successor = this
    _data = consume initial_data
    _successor_id = 0
    _predecessor_id = 0
    _main = main

    _env.out.print("Node " + _id.string() + " created with " + m.string() + " bit hash space and initial keys.")

    let stabilize_notify = ChordTimerNotify(_env, this, "stabilize")
    let stabilize_timer' = Timer(consume stabilize_notify, 5_000_000_000, 5_000_000_000) // 5 seconds
    _stabilize_timer = stabilize_timer'
    timers(consume stabilize_timer')

    let fix_fingers_notify = ChordTimerNotify(_env, this, "fix_fingers")
    let timer' = Timer(consume fix_fingers_notify, 1_000_000_000, 10_000_000_000) // 1 second, 10 seconds
    _timer = timer'
    timers(consume timer')

    let check_predecessor_notify = ChordTimerNotify(_env, this, "check_predecessor")
    let predecessor_check_timer' = Timer(consume check_predecessor_notify, 10_000_000_000, 10_000_000_000) // 10 seconds
    _predecessor_check_timer = predecessor_check_timer'
    timers(consume predecessor_check_timer')


be join(bootstrap_node: (Node | None)) =>
  _predecessor = None
  _predecessor_id = 0

  match bootstrap_node
  | let node: Node =>
      node.find_successor(_id, this, "find_successor")
  | None =>
      _env.out.print("No bootstrap node provided. Initializing as the first node in the network.")
      _successor = this
      _successor_id = _id
  end


  be receive_successor(successor: (Node | None), successor_id: U64) =>
    _env.out.print("Successor for node " + _id.string() + " updated to " + successor_id.string())
    _successor = successor
    _successor_id = successor_id
    _env.out.print("Notifying the successor of node " + _id.string())

    // // Use match to handle None and notify the successor
    // match _successor
    // | let succ: Node =>
    //   succ.notify(this,_id)
    // | None =>
    //   _env.out.print("No valid successor to notify.")
    // end

    // After finding the correct successor, propagate keys
    propagate_keys_to_correct_nodes()

  be propagate_keys_to_correct_nodes() =>
  // need to implement
      None



be find_successor(id: U64, requester: (Node | None), purpose: String = "find_successor", hop_count: U64 = 0, finger_index: USize = USize.max_value()) =>
  _env.out.print("Node " + _id.string() + " received find_successor request for ID " + id.string() + " with purpose: " + purpose + " and hop count: " + hop_count.string())

  if in_range(id, _id, _successor_id) then
    match purpose
    | "find_successor" =>
      match requester
      | let r: Node =>
        r.receive_successor(_successor, _successor_id)
      | None =>
        _env.out.print("Requester is None, cannot receive successor.")
      end
    | "lookup_key" =>
      match requester
      | let r: Node =>
        try
          let value = _data(id)?
          r.receive_lookup_result(id, value, hop_count + 1)
        else
          r.receive_lookup_result(id, None, hop_count + 1)
        end
      | None =>
        _env.out.print("Requester is None, cannot receive lookup result.")
      end
    | "update_finger" =>
      if finger_index != USize.max_value() then
        match requester
        | let r: Node =>
          _env.out.print("Updating finger table for finger " + finger_index.string())
          r.update_finger(finger_index, _successor, _successor_id)
        | None =>
          _env.out.print("Requester is None, cannot update finger table.")
        end
      else
        _env.out.print("Finger index not specified for finger table update.")
      end
    else
      _env.out.print("Unknown purpose in find_successor.")
    end
  else
    let closest_node = closest_preceding_node(id)

    match closest_node
    | let node: Node =>
      match purpose
      | "lookup_key" =>
          node.find_successor(id, requester, purpose, hop_count + 1)
      | "update_finger" =>
          node.find_successor(id, requester, purpose, 0, finger_index)
      | "find_successor" =>
          node.find_successor(id, requester, purpose)
      end
    | None =>
      _env.out.print("Error: closest_node is None, cannot call find_successor.")
    end
  end


  be lookup_key(key: U64) =>
    find_successor(key, this, "lookup_key", 0)

  be receive_lookup_result(key: U64, value: (String | None), hops: U64) =>
    if value is None then
      _env.out.print("Lookup result for key " + key.string() + " not found after " + hops.string() + " hops.")
    else
      _env.out.print("Lookup result for key " + key.string() + " found in " + hops.string() + " hops.")
      _main.receive_hop_count(hops)
    end



  // be find_successor(id: U64, requester: (Node | None), finger_index: USize = USize.max_value()) =>
  //   _env.out.print("Node " + _id.string() + " received find_successor request for ID " + id.string())


  //   if in_range(id, _id, _successor_id) then
     
  //     if finger_index != USize.max_value() then
  //       _env.out.print("Updating finger table for finger " + finger_index.string())
        
       
  //       match requester
  //       | let r: Node =>
  //         r.update_finger(finger_index, _successor, _successor_id)
  //       | None =>
  //         _env.out.print("Requester is None, cannot update finger table.")
  //       end
  //     else

  //       match requester
  //       | let r: Node =>
  //         r.receive_successor(_successor, _successor_id)
  //       | None =>
  //         _env.out.print("Requester is None, cannot receive successor.")
  //       end
  //     end
  //   else
  //     // Try to find the closest preceding node and catch any potential errors
  //     let closest_node = closest_preceding_node(id)

  //     match closest_node
  //     | let node: Node =>
  //         if finger_index != USize.max_value() then
  //             node.find_successor(id, requester, finger_index)
  //         else
  //             node.find_successor(id, requester)
  //         end
  //     | None =>
  //         _env.out.print("Error: closest_node is None, cannot call find_successor.")
  //     end

  //   end



  be fix_fingers() =>
    _env.out.print("Calling Fix Fingers")
    _next_finger = _next_finger + 1
    if _next_finger >= _m then
      _next_finger = 0
    end
    let offset = (_id + (1 << _next_finger).u64()) % (1 << _m).u64()
    _env.out.print("Fixing finger " + _next_finger.string() + " with offset " + offset.string())
    find_successor(offset, this, "update_finger",0, _next_finger)

  be update_finger(finger_index: USize, successor: (Node | None), id: U64) =>
    _env.out.print("Update finger")
    if finger_index < _m then
      _finger_table.insert(finger_index.u64(), successor)
    else
        _env.out.print("Error: Finger table index exceeds allowed size.")
    end




  be notify(caller: Node, caller_id: U64) =>

      if (_predecessor is None) or in_range(caller_id, _predecessor_id, _id) then
        _predecessor = caller
        _predecessor_id = caller_id
        _env.out.print("Predecessor updated to " + caller_id.string())
      else
        _env.out.print("Predecessor remains unchanged.")
      end


  fun closest_preceding_node(id: U64): (Node | None) =>
    let keys: Array[U64] = Array[U64]

    for key in _finger_table.keys() do
      keys.push(key)
    end

    Sort[Array[U64], U64](keys)

    var i: I64 = keys.size().i64() - 1
    while i >= 0 do
      try
        let finger_id: U64 = keys(i.usize())?
        
        if in_range(finger_id, _id, id) then
          return try _finger_table(finger_id)? else continue end
        end
      else
        _env.out.print("Error accessing keys or finger table. Continuing...")
      end

      i = i - 1
    end

    this








  be store_key(key: U64, value: String) =>
    _data(key) = value
    _env.out.print("Stored key " + key.string() + " with value '" + value.string() + "' at node " + _id.string())

  fun in_range(id: U64, id_start: U64, id_end: U64): Bool =>
    if id_start < id_end then
      (id > id_start )and (id <= id_end)
    else
      (id > id_start) or (id <= id_end)
    end

  be check_predecessor() =>
    _env.out.print("Checking if predecessor is alive.")

    match _predecessor
    | let pred: Node =>
      pred.alive(this)
    | None =>
      _env.out.print("Predecessor is None, setting to self.")
      _predecessor = this
      _predecessor_id = _id
    end
  

  be alive(response_to: Node) =>
    response_to.receive_alive_signal(this, _id)

  be receive_alive_signal(caller: Node, caller_id: U64) =>
    if _predecessor_id == caller_id then
      _env.out.print("Confirmed that predecessor " + caller_id.string() + " is alive.")
    else
      _env.out.print("Received alive signal from unknown node.")
    end


  be stabilize() =>
    _env.out.print("Stabilizing node " + _id.string())

    match _successor
    | let succ: Node =>

      succ.request_predecessor(this)
    | None =>
      _env.out.print("No successor to stabilize.")

    end



    be receive_predecessor(pred: (Node | None), pred_id: (U64 | None)) =>
      match pred_id
      | let actual_pred_id: U64 =>
          if in_range(actual_pred_id, _id, _successor_id) then
            _successor = pred
            _successor_id = actual_pred_id
            _env.out.print("Updated successor to " + actual_pred_id.string())
          end
      | None =>
          _env.out.print("Successor has no predecessor.")
      end
      

      match _successor
      | let succ: Node =>
          succ.notify(this, _id)
      | None =>
          _env.out.print("Cannot notify successor, as no successor exists.")
      end



  be request_predecessor(requestor: Node) =>
    requestor.receive_predecessor(_predecessor, _predecessor_id)
  
  be stop() =>
    timers.cancel(_timer)
    timers.cancel(_stabilize_timer)
    timers.cancel(_predecessor_check_timer)
    //     stabilize_notify.stop()
    // fix_fingers_notify.stop()
    // check_predecessor_notify.stop()
    

class ChordTimerNotify is TimerNotify
  let _node: Node
  let _task: String
  var _env: Env

  new iso create(env:Env, node: Node, task: String) =>
    _env = env
    _node = node
    _task = task

  fun ref apply(timer: Timer, count: U64): Bool =>
    match _task
    | "stabilize" =>
      _node.stabilize()
    | "fix_fingers" =>
      _node.fix_fingers()
    | "check_predecessor" =>
      _node.check_predecessor()
    else
      _env.out.print("Unknown task " + _task)
    end
    true
  
  // fun ref stop() =>
  //   _continue = false