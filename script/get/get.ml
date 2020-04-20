open Lwt.Infix

module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)

let readfile fileloc = 
let buf = Buffer.create 4096 in
try
    while true do
      let line = input_line fileloc in
      Buffer.add_string buf line;
      Buffer.add_char buf '\n';
    done;
    assert false 
  with
    End_of_file -> Buffer.contents buf

let getkeylist path = 
  let contentbuf = readfile (open_in path) in 
  let keylist = String.split_on_char('\n') contentbuf in 
  (* let keylist = List.tl (List.rev keylist) in   *)
  keylist

let rec getvalue keylist b_master =
	match keylist with 
    | h::t -> (try 
				ignore @@ Scylla_kvStore.get b_master [h] 
				(* >>= fun item ->
			  	print_string item ; Lwt.return_unit *)
			  with 
			  _ -> ());
			  (* Lwt.return_unit) *)
              getvalue t b_master
    | _ -> Lwt.return_unit

 

let _ =
let path = "/home/shashank/work/benchmark_irminscylla/input/heirarchical_keys/1l1k/keys" in
let conf = Irmin_scylla.config "127.0.0.1" in
Scylla_kvStore.Repo.v conf >>= fun repo ->
	Scylla_kvStore.master repo >>= fun b_master ->
		(* Scylla_kvStore.get b_master ["ztkmIyHy6x"; "XoEH0L7oEO"] >>= fun item -> *)
		
		let keylist = getkeylist path in 
		ignore @@getvalue keylist b_master;

		Lwt.return_unit
