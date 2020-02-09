open Lwt.Infix

(**Input format:
Depth level 1:
K1,V1
K2,V2

Depth level 2:
K1-K11,V1
K2-K22,V2 *)


(**format key function is to parse keys with depth level > 1 *)
let formatkey k v = 
  let klist = String.split_on_char('-') k in 
  (klist,v)


let rec func kvlist line_sep =
let kvpairlist = (match line_sep with
| p::p_lst -> (let pair = String.split_on_char(',') p in
        let a = (match pair with | k::v::[] -> formatkey k v | _ -> ([],"")) in
          func (a::kvlist) p_lst)
| _ -> kvlist) in
kvpairlist

let createKVPairList inputbuf =
  let line_sep = String.split_on_char('\n') inputbuf in
  let kvpairlist = func [] line_sep in
  kvpairlist

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

let kvpairfun path = 
  let contentbuf = readfile (open_in path) in 
  let kvpairlist = createKVPairList contentbuf in
  kvpairlist

(* ------------------------------------------------------------------- *)

module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)


let rec insIntoStore kvpairlist b_master = 
match kvpairlist with
|h::t -> let k, v = h in
        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) b_master k v;
        insIntoStore t b_master
| _ -> ()


(** The function reads the input file, parses the data into key and value, inserts it into the C* store and records the time for db operation. *)
let _ =
let path = "/home/shashank/work/createKVPairList_irminscylla/input/keydepth2/10-20-200/2000" in

let conf = Irmin_scylla.config "127.0.0.1" in
Scylla_kvStore.Repo.v conf >>= fun repo ->
	Scylla_kvStore.master repo >>= fun b_master ->

    let kvpairlist = kvpairfun path in
    let stime = Unix.gettimeofday() in (*ms*)
      insIntoStore kvpairlist b_master;
    let etime = Unix.gettimeofday() in
    let diff = etime -. stime in
    
    print_string "\ntime taken = "; print_float diff;

    Lwt.return_unit
	
