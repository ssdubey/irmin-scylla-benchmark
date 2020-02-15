open Lwt

module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)

let rec insertgraph kvpairlist b_master = 
match kvpairlist with
| h::t -> let k, v = h in 
        (* print_string ("key is = " ^ v); *)
        (* let k = Parse.formatkey k in *)
        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) b_master [k] v;
        insertgraph t b_master
| _ -> ()

let _ =
let path = "/home/shashank/work/benchmark_irminscylla/build_system/input/node3/input_libs" in

let conf = Irmin_scylla.config "172.17.0.3" in
Scylla_kvStore.Repo.v conf >>= fun repo ->
	Scylla_kvStore.master repo >>= fun b_master ->

    let kvpairlist = Parse.file_to_kvpair path in
    insertgraph kvpairlist b_master;
    
    Lwt.return_unit
