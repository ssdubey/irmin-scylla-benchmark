open Lwt.Infix

module Counter: Irmin.Contents.S with type t = int64 = struct
	type t = int64
	let t = Irmin.Type.int64
	let mc = ref 0
    let merge ~old a b = 
        (* Printf.printf "\n** merging %d %d" (Int64.to_int a) (Int64.to_int b); *)
        mc:=!mc+1;
	    let open Irmin.Merge.Infix in
		old () >|=* fun old ->
        let old = match old with None -> 0L | Some o -> o in
        let (+) = Int64.add and (-) = Int64.sub in 
        (* Printf.printf "  conflict:  %d  a=%d  b=%d  old=%d" !mc (Int64.to_int a) (Int64.to_int b) (Int64.to_int old); *)
        a + b - old
        
        let merge = Irmin.Merge.(option (v t merge))
end

module Scylla_kvStore = Irmin_scylla.KV(Counter)

let print msg client= () (*Printf.printf "\n** %s : %s" msg client*)

let updateMeta meta_name msg time = 
    let _, time_sum, cnt = !meta_name in
    let count = cnt + 1 in  
    let t = time_sum +. time in
    (* Printf.printf "\n** meta %s  %f  %d" msg t count; *)
    meta_name := (msg, t, count)

let mergeBranches outBranch currentBranch opr_meta = 
    let stime = Unix.gettimeofday () in 
        ignore @@ Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) outBranch ~into:currentBranch;
    let etime = Unix.gettimeofday () in
    (* Printf.printf "  *%f*" (etime -. stime); *)
        updateMeta opr_meta "mergebranches" (etime -. stime);

        Lwt.return_unit

let rec mergeOpr branchList currentBranch currentBranch_string repo opr_meta = 
    (* Printf.printf "\nthis is %s in mergeOpr" currentBranch_string; *)
    match branchList with 
    | h::t -> 
            (* Printf.printf "\ncurrent branch to merge: '%s'  currentbranch_string: '%s'" h currentBranch_string; *)
            if (currentBranch_string <> h) then (  
                (* Printf.printf "\n** merge of %s into %s" h currentBranch_string; *)
                Scylla_kvStore.of_branch repo h >>= fun branch -> 
                 
                    ignore @@ mergeBranches branch currentBranch opr_meta;
                    mergeOpr t currentBranch currentBranch_string repo opr_meta
                    )
                    else
                    mergeOpr t currentBranch currentBranch_string repo opr_meta
    | _ -> (*print_string "branch list empty";*) 
        Lwt.return_unit

let createValue () =
    Int64.of_int (Random.int 100)
    (* Int64.of_int 1 *)

let getvalue private_branch_anchor lib client get_meta =
    print "getvalue" client;
    let stime = Unix.gettimeofday() in
        Scylla_kvStore.get private_branch_anchor [lib] >>= fun item -> 
        (* Printf.printf "\nclient= %s  key=%s  value=%d" client lib (Int64.to_int item);  *)
        ignore item;
    let etime = Unix.gettimeofday() in
    updateMeta get_meta "getvalue" (etime -. stime);
    
    Lwt.return_unit

let rec build liblist private_branch_anchor repo client set_meta get_meta rw = (*cbranch_string as in current branch is only used for putting string in db*)
    match liblist with 
    | lib :: libls -> 
        (* find_in_db lib private_branch_anchor >>= fun boolval ->  *)
             
        (match rw with 
            | "post_write" -> (print "post_write build" client;
                        let v = createValue () in
                        let stime = Unix.gettimeofday() in
                        
                        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                private_branch_anchor [lib] v;
                        
                        let etime = Unix.gettimeofday() in
                        let diff = etime -. stime in
                        updateMeta set_meta "build_write" diff;
                         
                        )

            | "read" -> print "read build" client;
                ignore @@ getvalue private_branch_anchor lib client get_meta
            
            | _ -> failwith "wrong option for rw");

        build libls private_branch_anchor repo client set_meta get_meta rw;

    | [] -> Lwt.return_unit


(*generating key of 2B *)
let gen_write_key () = 
  let str = [|"1";"2";"3";"4";"5";"6";"7";"8";"9";"0";"a";"b";"c";"d";"e";"f";"g";"h";"i";"j";"k";"l";"m";"n";"o";"p";"q";"r";"s";"t";"u";"v";|] in
  let key = (Array.get str (Random.int 32))^(Array.get str (Random.int 32)) in
  key


(*generating 2B random key from limited keyspace*)
let rec generate_write_key_list count = 
  if (count>0) then (
      gen_write_key () :: generate_write_key_list (count -1) )
  else
      []
  
let create_or_get_private_branch repo ip = 
  try
  Scylla_kvStore.of_branch repo (ip ^ "_private") (*this should never fail*)
  with _ -> 
  Printf.printf "\nget private branch failed which is an error...";
  Scylla_kvStore.master repo >>= fun b_master ->
  Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_private")

let create_or_get_public_branch repo ip = 
  try
  Scylla_kvStore.of_branch repo (ip ^ "_public")
  with _ -> 
  Scylla_kvStore.master repo >>= fun b_master ->
  Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_public")

let publish branch1 branch2 publish_meta client = (*changes of branch2 will merge into branch1*)
    (* Printf.printf "\npublish of %s" branch2; *)
    print "publish" client;
    let stime = Unix.gettimeofday() in
        ignore @@ Scylla_kvStore.merge_with_branch ~info:(fun () -> Irmin.Info.empty) branch1 branch2;
    let etime = Unix.gettimeofday() in
    let diff = etime -. stime in
    (* Printf.printf "  (%f)" diff; *)
    updateMeta publish_meta "publish_merge" diff

let publish_to_public repo ip publish_meta = 
    print "publish_to_public" ip;
    (* Printf.printf "\npublishing..."; *)
    create_or_get_public_branch repo ip >>= fun public_branch_anchor ->
    (*changes of 2nd arg branch will merge into first*)
    ignore @@ publish public_branch_anchor (ip ^ "_private") publish_meta ip;
    Lwt.return_unit 


let filter_str str =
  let split = String.split_on_char '_' str in 
  let split = List.rev split in
  let status = List.hd split in
  if status="public" then true else false

let filter_public branchList =
  List.filter filter_str branchList


let refresh repo client refresh_meta =
  print "refresh" client;
  (* Printf.printf "\nrefreshing..."; *)
  (*merge current branch with the detached head of other*) 
  create_or_get_public_branch repo client >>= fun public_branch_anchor ->
  Scylla_kvStore.Branch.list repo >>= fun branchList -> 
  (* List.iter (fun x -> Printf.printf " *%s " x) branchList; *)
  let branchList = filter_public branchList in
  (* List.iter (fun x -> Printf.printf " ~%s " x) branchList; *)

  mergeOpr branchList public_branch_anchor (client ^ "_public") repo refresh_meta  (*merge is returning unit*)

  (* create_or_get_private_branch repo client >>= fun private_branch_anchor ->
  mergeBranches public_branch_anchor private_branch_anchor refresh_meta                                                                                                  *)


(* let squash repo private_branch_str old_commit = 
  Scylla_kvStore.Branch.get repo private_branch_str >>= fun latest_cmt ->
  let latest_tree = Scylla_kvStore.Commit.tree latest_cmt in

  (* Scylla_kvStore.Branch.get repo public_branch_str >>= fun old_commit -> *)
  Scylla_kvStore.Commit.v repo ~info:(Irmin.Info.empty) ~parents:[Scylla_kvStore.Commit.hash old_commit] latest_tree >>= fun new_commit -> 
  (* print "%s" (Irmin.Type.to_string (Scylla_kvStore.Commit.t repo) new_commit); *)

  Scylla_kvStore.of_branch repo private_branch_str >>= fun private_branch_anchor ->
  Scylla_kvStore.Head.set private_branch_anchor new_commit *)


let post_operate_help opr_load private_branch_anchor repo client total_opr_load flag set_meta get_meta publish_meta refresh_meta old_commit =
  (* Printf.printf "\nPost: client %s:" client; *)
  print "post_operate_help" client;
  let write_keylist = generate_write_key_list opr_load in (*generate_write_key_list is generating key for write operation*)
  (* Printf.printf "\n";
  List.iter (fun key -> Printf.printf "k=%s " key) write_keylist; *)
  ignore @@ build write_keylist private_branch_anchor repo client set_meta get_meta "post_write";

  ignore @@ build write_keylist private_branch_anchor repo client set_meta get_meta "read";
  (* ignore @@ (create_or_get_public_branch repo client >>= fun public_branch_anchor -> *)
(* Printf.printf " publishing... "; *)
  ignore @@ publish_to_public repo client publish_meta;
  
  (* ignore @@ build write_keylist public_branch_anchor repo client set_meta get_meta "read"; *)

(* Printf.printf " squashing... ";
  ignore (old_commit >>= fun old_commit ->
                  squash repo (client^"_public") old_commit); *)

(* Printf.printf " refreshing... "; *)
  ignore @@ refresh repo client refresh_meta
  (* ignore @@ build write_keylist public_branch_anchor repo client set_meta get_meta "read"; *)
  (* Lwt.return_unit) *)
  

let rec operate opr_load private_branch_anchor repo client total_opr_load flag done_opr set_meta get_meta publish_meta refresh_meta loop_count old_commit =
  print "operate" client;
  let rw_load = opr_load/2 in
  Random.init (1); (*so that each client get the same read keylist*)

  Random.init ((Unix.getpid ()) + loop_count);
  post_operate_help rw_load private_branch_anchor repo client total_opr_load flag set_meta get_meta publish_meta refresh_meta old_commit;
      
  (*this will make the keys generated in 2^x groups*)
  let new_opr_load, flag = 
  if (done_opr + (2 * opr_load)) < total_opr_load then
      ((2 * opr_load), true)
  else    
      ((total_opr_load - done_opr), false)
  in
  
  let done_opr = done_opr + new_opr_load in

  if flag=true then (*flag denotes if it is a last round of operation or not. true = more rounds are there, false = no more rounds*)
      let loop_count = loop_count + 1 in
      operate new_opr_load private_branch_anchor repo client total_opr_load flag done_opr set_meta get_meta publish_meta refresh_meta loop_count old_commit
  else  if new_opr_load != 0 then(
      let rw_load = new_opr_load/2 in
      Random.init (1); (*so that each client get the same read keylist*)
      
      let loop_count = loop_count + 1 in
      Random.init ((Unix.getpid ()) + loop_count);
      post_operate_help rw_load private_branch_anchor repo client total_opr_load flag set_meta get_meta publish_meta refresh_meta old_commit
  
  )  
      (* operate_help new_opr_load private_branch_anchor repo client total_opr_load flag set_meta get_meta *)


let special_fun repo client = 
  (* create_or_get_public_branch repo client >>= fun public_branch_anchor ->
  
  ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
  public_branch_anchor ["AA"] (Int64.of_int 11);

  Scylla_kvStore.Branch.get repo (client^"_public") >>= fun old_commit -> *)
  Lwt.return "old_commit"


let buildLibrary ip client total_opr_load set_meta get_meta publish_meta refresh_meta =
  print "buildlibrary" client;
  let conf = Irmin_scylla.config ip in
  Scylla_kvStore.Repo.v conf >>= fun repo ->
  
  create_or_get_private_branch repo client >>= fun private_branch_anchor ->
  
  let old_commit = special_fun repo client in
  (* ignore old_commit; *)
  let opr_load = 2 in 
  let done_opr = 2 in
  operate opr_load private_branch_anchor repo client total_opr_load true done_opr set_meta get_meta publish_meta refresh_meta 0 old_commit;
  (* ignore @@ refresh repo client refresh_meta; *)
  Lwt.return_unit 

let _ =
  let hostip = Sys.argv.(1) in
  let client = Sys.argv.(2) in
  let total_opr_load = Sys.argv.(3) in (* no. of keys to insert *)
  print "beginning" client;
  Random.init (Unix.getpid ());

  let set_meta = ref ("", 0.0, 0) in 
  let get_meta = ref ("", 0.0, 0) in 
  (* let merge_meta = ref ("", 0.0, 0) in  *)
  let publish_meta = ref ("", 0.0, 0) in 
  let refresh_meta = ref ("", 0.0, 0) in 

  
  ignore @@ buildLibrary hostip client (int_of_string total_opr_load) set_meta get_meta publish_meta refresh_meta;

  let (set_msg, set_time, set_count) = !set_meta in 
  let (get_msg, get_time, get_count) = !get_meta in 
  let (publish_msg, publish_time, publish_count) = !publish_meta in 
  let (refresh_msg, refresh_time, refresh_count) = !refresh_meta in 
  
  let total_time = set_time +. get_time +. publish_time +. refresh_time in 
  (* Printf.printf "\n\nset_time = %f;set_count = %d;get_time = %f;get_count = %d;publish_time = %f;publish_count = %d;refresh_time = %f;refresh_count = %d;total_time = %f" set_time set_count get_time get_count publish_time publish_count refresh_time refresh_count total_time; *)

  Printf.printf "\n%f; %d; %f; %d; %f; %d; %f; %d; %f" set_time set_count get_time get_count publish_time publish_count refresh_time refresh_count total_time;
  