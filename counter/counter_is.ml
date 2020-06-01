open Lwt.Infix

module Counter: Irmin.Contents.S with type t = int64 = struct
	type t = int64
	let t = Irmin.Type.int64
	
	let merge ~old a b = print_string "\nmerging";
	    let open Irmin.Merge.Infix in
		old () >|=* fun old ->
        let old = match old with None -> 0L | Some o -> o in
        let (+) = Int64.add and (-) = Int64.sub in
        a + b - old
        
        let merge = print_string "\nmerge fun called";Irmin.Merge.(option (v t merge))
end

let merge = Irmin.Merge.(option counter)


module Scylla_kvStore = Irmin_scylla.KV(Counter)

let mergeBranches outBranch currentBranch = 
    Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) outBranch ~into:currentBranch

let rec mergeOpr branchList currentBranch repo =
    match branchList with 
    | h::t -> print_string ("\ncurrent branch to merge: " ^ h);Scylla_kvStore.of_branch repo h >>= fun branch ->    
                ignore @@ mergeBranches branch currentBranch;
                mergeOpr t currentBranch repo 
    | _ -> print_string "branch list empty"; Lwt.return_unit

let createValue () =
    Int64.of_int 1

let find_in_db lib private_branch = 
    try 
    let stime = Unix.gettimeofday () in 
    Scylla_kvStore.get private_branch [lib] >>= fun _ ->
    let etime = Unix.gettimeofday () in
    Printf.printf "\nfind_in_db: %f" (etime -. stime);
    Lwt.return_true
    with 
    _ -> Lwt.return_false

let getvalue private_branch_anchor lib =
    Scylla_kvStore.get private_branch_anchor [lib] >>= fun item -> Printf.printf "\nkey=%s  value=%d" lib (Int64.to_int item); Lwt.return_unit

let rec build liblist private_branch_anchor cbranch_string repo ip = (*cbranch_string as in current branch is only used for putting string in db*)
    match liblist with 
    | lib :: libls -> 
        find_in_db lib private_branch_anchor >>= fun boolval -> 
            (match boolval with 
            | false -> (let v = createValue () in
                        let stime = Unix.gettimeofday() in
                        
                        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                private_branch_anchor [lib] v;
                        
                        getvalue private_branch_anchor lib;
                        
                        let etime = Unix.gettimeofday() in
                        let diff = etime -. stime in
                        (* print_string "\ntime taken in inserting one key = ";  *)
                        (* getvalue private_branch_anchor lib; *)
                        Printf.printf "\nfalse_setting: %f" diff;)
                        (*print_float (diff);*)

            | true -> 
            getvalue private_branch_anchor lib;
            ());

        build libls private_branch_anchor cbranch_string repo ip;

    | [] -> Lwt.return_unit

let readfile fileloc = 
    let buf = Buffer.create 4096 in
    try
        while true do
        let line = input_line fileloc in
        Buffer.add_string buf line;
        Buffer.add_char buf '\n'
        done;
        assert false 
    with
        End_of_file -> Buffer.contents buf

(*generating key of 2B *)
let gen () = 
    let str = [|"1";"2";"3";"4";"5";"6";"7";"8";"9";"0";"a";"b";"c";"d";"e";"f";"g";"h";"i";"j";|] in
    let key = (Array.get str (Random.int 2))^(Array.get str (Random.int 2)) in
    key
    
(*generating 2B random key from limited keyspace*)
let rec generatekey count = 
    if (count>0) then (
        gen () :: generatekey (count -1) )
    else
        []
    
let create_or_get_private_branch repo ip = 
    try
    Scylla_kvStore.of_branch repo (ip ^ "_private")
    with _ -> 
    Scylla_kvStore.master repo >>= fun b_master ->
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_private")

let create_or_get_public_branch repo ip = 
    try
    Scylla_kvStore.of_branch repo (ip ^ "_public")
    with _ -> 
    Scylla_kvStore.master repo >>= fun b_master ->
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_public")

let publish branch1 branch2 = (*changes of branch2 will merge into branch1*)
    Scylla_kvStore.merge_with_branch ~info:(fun () -> Irmin.Info.empty) branch1 branch2

let publish_to_public repo ip =
    create_or_get_public_branch repo ip >>= fun public_branch_anchor ->
    (*changes of 2nd arg branch will merge into first*)
    ignore @@ publish public_branch_anchor (ip ^ "_private");
    Lwt.return_unit 

let refresh repo client =
    (*merge current branch with the detached head of other*) 
    create_or_get_public_branch repo client >>= fun public_branch_anchor ->
    Scylla_kvStore.Branch.list repo >>= fun branchList -> 
    mergeOpr branchList public_branch_anchor repo;  (*merge is returning unit*)

    create_or_get_private_branch repo client >>= fun private_branch_anchor ->
    mergeBranches public_branch_anchor private_branch_anchor                                                                                                  

let operate_help opr_load private_branch_anchor repo client total_opr_load flag =
    let keylist = generatekey opr_load in

    ignore @@ build keylist private_branch_anchor (client ^ "_private") repo client;

    ignore @@ publish_to_public repo client;

    ignore @@ refresh repo client

let rec operate opr_load private_branch_anchor repo client total_opr_load flag done_opr =
    
    operate_help opr_load private_branch_anchor repo client total_opr_load flag;
        
    let new_opr_load, flag = 
    if (done_opr + (2 * opr_load)) < total_opr_load then
        ((2 * opr_load), true)
    else    
        ((total_opr_load - done_opr), false)
    in
    
    let done_opr = done_opr + new_opr_load in

    if flag=true then
        operate new_opr_load private_branch_anchor repo client total_opr_load flag done_opr
    else    
        operate_help new_opr_load private_branch_anchor repo client total_opr_load flag

let buildLibrary ip client total_opr_load =
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
    
    create_or_get_private_branch repo client >>= fun private_branch_anchor ->
    
    (*running loop to execute operation in (2^x) count. For each loop fresh set of keys will be generated*)
    let opr_load = 2 in 
    let done_opr = 2 in
    operate opr_load private_branch_anchor repo client total_opr_load true done_opr;
    
    Lwt.return_unit 


let _ =
        let hostip = Sys.argv.(1) in
        let client = Sys.argv.(2) in
        let total_opr_load = Sys.argv.(3) in (* no. of keys to insert *)
        
        Random.init (Unix.getpid ());

        buildLibrary hostip client (int_of_string total_opr_load)

   