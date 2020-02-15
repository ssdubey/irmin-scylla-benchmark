open Lwt.Infix

let readfile fileloc = 
let buf = Buffer.create 4096 in
try
    while true do
      let line = input_line fileloc in
      Buffer.add_string buf line;
      Buffer.add_char buf '\n'
    done;
    assert false (* This is never executed
                    (always raise Assert_failure). *)
  with
    End_of_file -> Buffer.contents buf


module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)

let find_in_db lib b_master = 
    try 
    Scylla_kvStore.get b_master [lib] >>= fun item ->
    print_string ("\n" ^ item);
    Lwt.return_unit
    with 
    _ -> print_string ("\n " ^ lib ^ " downloading...");
        print_string ("\ndownloaded " ^ lib ^ " in build");

    Lwt.return_unit
    
let rec findlib liblist b_master = 
    match liblist with
    | _::[] -> ()  (*since the last element is empty*)
    | h::t -> ignore @@ find_in_db h b_master; 
                findlib t b_master
    | _ -> ()


let _ =

(*
Read the list of libraries needed
for each library, search if it is present in the db
if the library is not present download it
if present or downloaded, put it all in the same file. *)

let path = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildnode/libreq" in

let conf = Irmin_scylla.config "172.17.0.2" in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
        Scylla_kvStore.master repo >>= fun b_master ->

        let fileContentBuf = readfile (open_in path) in 
            (* print_string fileContentBuf; *)
            let liblist = String.split_on_char('\n') fileContentBuf in 
             (* print_string ("no of libs = "); print_int (List.length liblist);  *)
                findlib liblist b_master;

Lwt.return_unit