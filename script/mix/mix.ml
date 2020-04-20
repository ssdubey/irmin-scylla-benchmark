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

let getkeylist path = 
  let contentbuf = readfile (open_in path) in 
  let keylist = String.split_on_char('\n') contentbuf in 
  keylist

(* ------------------------------------------------------------------- *)

module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)


(* let get b_master keylist =
Scylla_kvStore.get b_master keylist >>= fun item -> 
		(* print_string item; *)
		Lwt.return_unit *)

(*1=choose in both 4=all done 2=only set 3=only get*)
let rec mix kvpairlist keylist b_master instruct =

  let choice = match instruct with 
  | 1 -> (Random.int 10) mod 2 
  | 2 -> 0 
  | 3 -> 1
  | 4 -> 4 
  | _ -> 4 in 

  if choice=4 then ()
  else if choice=0 then (

  match kvpairlist with
  |h::t -> let k, v = h in
          let stime = Unix.gettimeofday() in
          ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) b_master k v;
          let etime = Unix.gettimeofday() in
          let diff = etime -. stime in 
          (* print_string "\ntime taken in inserting one key = ";  *)
          print_string "\n";
          print_float (diff);
          mix t keylist b_master 1;
  | _ -> if (instruct=2) then mix kvpairlist keylist b_master 4
        else mix kvpairlist keylist b_master 3

  )
  else (

    match keylist with 
    | h::t -> (try 
              ignore @@ Scylla_kvStore.get b_master [h] ; 
              with 
              _ -> ());
              mix kvpairlist t b_master 1
    | _ -> if (instruct=3) then mix kvpairlist keylist b_master 4
        else mix kvpairlist keylist b_master 2

  )

(** The function reads the input file, parses the data into key and value, inserts it into the C* store and records the time for db operation. *)
let _ =
let kv_path = "/home/shashank/work/benchmark_irminscylla/input/heirarchical_keys/2l1k/kv" in
let key_path = "/home/shashank/work/benchmark_irminscylla/input/heirarchical_keys/1l1k/keys" in

let conf = Irmin_scylla.config "172.17.0.2" in
Scylla_kvStore.Repo.v conf >>= fun repo ->
	Scylla_kvStore.master repo >>= fun b_master ->

    let kvpairlist = kvpairfun kv_path in
    let keylist = getkeylist key_path in (*keylist would be string which can be just put in list*)

let instruct = 1 in (*1=choose in both 0=all done 2=only set 3=only get*)

let stime = Unix.gettimeofday() in 
  mix kvpairlist keylist b_master instruct;
let etime = Unix.gettimeofday() in
let diff = etime -. stime in
print_string "\n\n";
print_float diff;



    Lwt.return_unit
	
