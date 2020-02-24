
let _ =
    let ip = "172.17.0.3" in
    let liblistpath = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildsystem/libreq_node2" in 
    Buildsystem.buildLibrary ip liblistpath
    (* Buildsystem.main () *)
