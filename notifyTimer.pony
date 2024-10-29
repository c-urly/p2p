use "time"

class ChordTimerNotify is TimerNotify
    let _node: Node
    let _task: String
    var _env: Env
    var _id: U64
  new iso create(env:Env, node: Node, id: U64, task: String) =>
    _env = env
    _node = node
    _task = task
    _id = id

  fun ref apply(timer: Timer, count: U64): Bool =>
    match _task
    | "stabilize" =>
        // _env.out.print("Stabalize Timer Executing "+ _id.string() )
        _node.stabilize()
    | "fix_fingers" =>
        // _env.out.print("Fix Fingers Timer Executing " + _id.string())
        _node.fix_fingers()
    | "check_predecessor" =>
        // _env.out.print("Check Predecessor Timer Executing " + _id.string())
        _node.check_predecessor()
    else
      _env.out.print("Unknown task " + _task)
    end
    true