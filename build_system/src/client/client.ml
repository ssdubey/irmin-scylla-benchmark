
let _ =
    let ip = "172.17.0.2" in
    let liblistpath = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildsystem/libreq" in 
    Buildsystem.buildLibrary ip liblistpath
