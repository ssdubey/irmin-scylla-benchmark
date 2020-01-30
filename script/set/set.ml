open Lwt.Infix

(* ---------------------------------------------------------------------- *)
let rec func kvlist line_sep =
let kvpairlist = (match line_sep with
| p::p_lst -> (let pair = String.split_on_char(',') p in
        let a = (match pair with | k::v::[] -> (k,v) | _ -> ("","")) in
          func (a::kvlist) p_lst)
| _ -> kvlist) in
(* print_string ("\nkvpairlist length in rec func: " ); print_int (List.length kvpairlist); *)
kvpairlist

let benchmark inputbuf =
  let line_sep = String.split_on_char('\n') inputbuf in
  print_string ("\nno. of entries: " ); print_int (List.length line_sep);
  let kvpairlist = func [] line_sep in
  kvpairlist

(* let rec fprint kvpairlist = 
match kvpairlist with
|h::t -> let k, v = h in
        print_string ("\nkey =" ^ k); print_string ("value =" ^ v) ;
        fprint t
| _ -> () *)

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

(* let _ = *)
let kvpairfun path = 
  let contentbuf = readfile (open_in path) in 
  let kvpairlist = benchmark contentbuf in
(* print_string ("\nkvpairlist length in main:" );print_int (List.length kvpairlist);
fprint kvpairlist *)
kvpairlist

(* ------------------------------------------------------------------- *)

module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)


let rec fprintf kvpairlist b_master = 
match kvpairlist with
|h::t -> let k, v = h in
        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) b_master [k] v;
        fprintf t b_master
| _ -> ()

let _ =
let path = "/home/shashank/work/benchmark_irminscylla/input/10-50/10000" in

let conf = Irmin_scylla.config "172.17.0.2" in
Scylla_kvStore.Repo.v conf >>= fun repo ->
	Scylla_kvStore.master repo >>= fun b_master ->

    let kvpairlist = kvpairfun path in
    (* print_string ("\nkvpairlist length in main:" );print_int (List.length kvpairlist); *)
    let stime = Unix.gettimeofday() in (*ms*)
    fprintf kvpairlist b_master;
    let etime = Unix.gettimeofday() in
    let diff = etime -. stime in
    print_string "\ntime taken = "; print_float diff;

    Lwt.return_unit
	