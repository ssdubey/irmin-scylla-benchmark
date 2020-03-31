
let _ =
    let ip = "127.0.0.1" in
    let liblistpath = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildsystem/libreq_node2" in 
    Buildsystem.buildLibrary ip liblistpath
    (* Buildsystem.main () *)
