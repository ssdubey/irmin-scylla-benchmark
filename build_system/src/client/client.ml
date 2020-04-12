
let _ =
    let ip = "127.0.0.1" in
    let liblistpath = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildsystem/libreq2" in 
    Buildsystem.buildLibrary ip liblistpath
    (* Buildsystem.main () *)
