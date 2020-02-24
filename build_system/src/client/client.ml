
let _ =
    let ip = "172.17.0.3" in
    let liblistpath = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildsystem/libreq3" in 
    Buildsystem.buildLibrary ip liblistpath
    (* Buildsystem.main () *)
