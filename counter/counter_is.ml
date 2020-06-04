open Lwt.Infix

module Counter: Irmin.Contents.S with type t = int64 = struct
	type t = int64
	let t = Irmin.Type.int64
	
	let merge ~old a b = (*print_string "\nmerging";*)
	    let open Irmin.Merge.Infix in
		old () >|=* fun old ->
        let old = match old with None -> 0L | Some o -> o in
        let (+) = Int64.add and (-) = Int64.sub in
        a + b - old
        
        let merge = Irmin.Merge.(option (v t merge))
end

module Scylla_kvStore = Irmin_scylla.KV(Counter)

let updateMeta meta_name msg time = 
    let _, time_sum, cnt = !meta_name in
    let count = cnt + 1 in  
    let t = time_sum +. time in
    meta_name := (msg, t, count)

let mergeBranches outBranch currentBranch merge_meta = 
    let stime = Unix.gettimeofday () in 
        ignore @@ Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) outBranch ~into:currentBranch;
    let etime = Unix.gettimeofday () in
        updateMeta merge_meta "mergebranches" (etime -. stime);

        Lwt.return_unit

let rec mergeOpr branchList currentBranch repo merge_meta =
    match branchList with 
    | h::t -> (*print_string ("\ncurrent branch to merge: " ^ h);*)
                Scylla_kvStore.of_branch repo h >>= fun branch ->    
                ignore @@ mergeBranches branch currentBranch merge_meta;
                mergeOpr t currentBranch repo merge_meta
    | _ -> (*print_string "branch list empty";*) 
        Lwt.return_unit

let createValue () =
    Int64.of_int (Random.int 100)

let getvalue private_branch_anchor lib client get_meta =
    let stime = Unix.gettimeofday() in
        Scylla_kvStore.get private_branch_anchor [lib] >>= fun item -> 
        (* Printf.printf "\nclient= %s  key=%s  value=%d\n" client lib (Int64.to_int item);  *)
        ignore item;
    let etime = Unix.gettimeofday() in
    updateMeta get_meta "getvalue" (etime -. stime);
    
    Lwt.return_unit

let rec build liblist private_branch_anchor repo client set_meta get_meta rw = (*cbranch_string as in current branch is only used for putting string in db*)
    match liblist with 
    | lib :: libls -> 
        (* find_in_db lib private_branch_anchor >>= fun boolval ->  *)
             
        (match rw with 
            | "write" -> (let v = createValue () in
                        let stime = Unix.gettimeofday() in
                        
                        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                private_branch_anchor [lib] v;
                        
                        let etime = Unix.gettimeofday() in
                        let diff = etime -. stime in
                        updateMeta set_meta "build_write" diff;
                                                
                        (* getvalue private_branch_anchor lib client get_meta;
                        Printf.printf "\nfalse_setting: %f" diff; *)
                        
                        )

            | "read" -> 
                ignore @@ getvalue private_branch_anchor lib client get_meta
            
            | _ -> failwith "wrong option for rw");

        build libls private_branch_anchor repo client set_meta get_meta rw;

    | [] -> Lwt.return_unit

(*generating key of 2B *)
let gen_write_key () = 
    let str = [|"1";"2";"3";"4";"5";"6";"7";"8";"9";"0";"a";"b";"c";"d";"e";"f";"g";"h";"i";"j";|] in
    let key = (Array.get str (Random.int 20))^(Array.get str (Random.int 20)) in
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

let publish branch1 branch2 merge_meta = (*changes of branch2 will merge into branch1*)
    let stime = Unix.gettimeofday() in
        ignore @@ Scylla_kvStore.merge_with_branch ~info:(fun () -> Irmin.Info.empty) branch1 branch2;
    let etime = Unix.gettimeofday() in
    let diff = etime -. stime in
    updateMeta merge_meta "publish_merge" diff

let publish_to_public repo ip merge_meta = 
    (* Printf.printf "\npublishing..."; *)
    create_or_get_public_branch repo ip >>= fun public_branch_anchor ->
    (*changes of 2nd arg branch will merge into first*)
    ignore @@ publish public_branch_anchor (ip ^ "_private") merge_meta;
    Lwt.return_unit 

let refresh repo client merge_meta =
    (* Printf.printf "\nrefreshing..."; *)
    (*merge current branch with the detached head of other*) 
    create_or_get_public_branch repo client >>= fun public_branch_anchor ->
    Scylla_kvStore.Branch.list repo >>= fun branchList -> 
    ignore @@ mergeOpr branchList public_branch_anchor repo merge_meta;  (*merge is returning unit*)

    create_or_get_private_branch repo client >>= fun private_branch_anchor ->
    mergeBranches public_branch_anchor private_branch_anchor merge_meta                                                                                                 

let post_operate_help opr_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta read_keylist =
    (* Printf.printf "\nPost: client %s:" client; *)
    let write_keylist = generate_write_key_list opr_load in (*generate_write_key_list is generating key for write operation*)
    
    (* Printf.printf "\nread keylist: ";
    List.iter (fun key -> Printf.printf "%s " key) read_keylist;

    Printf.printf "\nwrite keylist: ";
    List.iter (fun key -> Printf.printf "%s " key) write_keylist; *)

    ignore @@ build read_keylist private_branch_anchor repo client set_meta get_meta "read";

    ignore @@ build write_keylist private_branch_anchor repo client set_meta get_meta "write";

    ignore @@ publish_to_public repo client merge_meta;

    ignore @@ refresh repo client merge_meta


let pre_operate_help opr_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta keylist =
    (* let keylist = generate_write_key_list opr_load in *)
    (* Printf.printf "\nPre: client %s:" client; *)
    (* List.iter (fun key -> Printf.printf "%s " key) keylist; *)

    ignore @@ build keylist private_branch_anchor repo client set_meta get_meta "write"

let gen_read_key () = 
    let str = [|"k";"l";"m";"n";"o";"p";"q";"r";"s";"t";"u";"v";"w";"x";"y";"z";"A";"B";"C";"D";|] in
    let key = (Array.get str (Random.int 20))^(Array.get str (Random.int 20)) in
    key

let rec gen_read_key_list count =
    if count > 0 then (
        let key = gen_read_key () in 
        key :: (gen_read_key_list (count-1) )
    )else
    []

let rec operate opr_load private_branch_anchor repo client total_opr_load flag done_opr set_meta get_meta merge_meta loop_count =
    
    let rw_load = opr_load/2 in
    Random.init (1); (*so that each client get the same read keylist*)

    let read_keylist = gen_read_key_list rw_load in
    pre_operate_help rw_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta read_keylist; 

    Random.init ((Unix.getpid ()) + loop_count);
    post_operate_help rw_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta read_keylist;
        
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
        operate new_opr_load private_branch_anchor repo client total_opr_load flag done_opr set_meta get_meta merge_meta loop_count
    else  (
        let rw_load = new_opr_load/2 in
        Random.init (1); (*so that each client get the same read keylist*)
        let read_keylist = gen_read_key_list rw_load in
        pre_operate_help rw_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta read_keylist; 

        let loop_count = loop_count + 1 in
        Random.init ((Unix.getpid ()) + loop_count);
        post_operate_help rw_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta read_keylist
    
    )  
        (* operate_help new_opr_load private_branch_anchor repo client total_opr_load flag set_meta get_meta merge_meta *)

let buildLibrary ip client total_opr_load set_meta get_meta merge_meta =
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
    
    create_or_get_private_branch repo client >>= fun private_branch_anchor ->
    
    let opr_load = 2 in 
    let done_opr = 2 in
    operate opr_load private_branch_anchor repo client total_opr_load true done_opr set_meta get_meta merge_meta 0;
    
    Lwt.return_unit 

let _ =
        let hostip = Sys.argv.(1) in
        let client = Sys.argv.(2) in
        let total_opr_load = Sys.argv.(3) in (* no. of keys to insert *)
        
        Random.init (Unix.getpid ());

        let set_meta = ref ("", 0.0, 0) in 
        let get_meta = ref ("", 0.0, 0) in 
        let merge_meta = ref ("", 0.0, 0) in 

        
        ignore @@ buildLibrary hostip client (int_of_string total_opr_load) set_meta get_meta merge_meta;

        let (set_msg, set_time, set_count) = !set_meta in 
        let (get_msg, get_time, get_count) = !get_meta in 
        let (merge_msg, merge_time, merge_count) = !merge_meta in 
        
        Printf.printf "\n\nset_time = %f  set_count = %d" set_time set_count;

        Printf.printf "\n\nget_time = %f  get_count = %d" get_time get_count;

        Printf.printf "\n\nmerge_time = %f  merge_count = %d" merge_time merge_count

        (* Printf.printf "\n\nset_msg = %s  set_time = %f  set_count = %d" set_msg set_time set_count;

        Printf.printf "\n\nget_msg = %s  get_time = %f  get_count = %d" get_msg get_time get_count;

        Printf.printf "\n\nmerge_msg = %s  merge_time = %f  merge_count = %d" merge_msg merge_time merge_count *)

   