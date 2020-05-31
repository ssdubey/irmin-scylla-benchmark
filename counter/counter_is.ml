open Lwt.Infix

module Counter: Irmin.Contents.S with type t = int64 = struct
	type t = int64
	let t = Irmin.Type.int64
	
	let merge ~old a b = print_string "merging";
	    let open Irmin.Merge.Infix in
		old () >|=* fun old ->
        let old = match old with None -> 0L | Some o -> o in
        let (+) = Int64.add and (-) = Int64.sub in
        a + b - old
        
        let merge = Irmin.Merge.(option (v t merge))
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
    Scylla_kvStore.get private_branch_anchor [lib] >>= fun item -> print_string "\n"; print_int (Int64.to_int item); Lwt.return_unit

let rec build liblist private_branch_anchor cbranch_string repo ip = (*cbranch_string as in current branch is only used for putting string in db*)
    match liblist with 
    | lib :: libls -> 
        find_in_db lib private_branch_anchor >>= fun boolval -> 
            (match boolval with 
            | false -> (let v = createValue () in
                        let stime = Unix.gettimeofday() in
                        
                        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                private_branch_anchor [lib] v;
                        
                        let etime = Unix.gettimeofday() in
                        let diff = etime -. stime in
                        (* print_string "\ntime taken in inserting one key = ";  *)
                        getvalue private_branch_anchor lib;
                        Printf.printf "\nfalse_setting: %f" diff;)
                        (*print_float (diff);*)

            | true -> getvalue private_branch_anchor lib; ());
            
            (* ()       );                              *)

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

(* let file_to_liblist liblistpath = 
    let fileContentBuf = readfile (open_in liblistpath) in 
    let liblist = String.split_on_char('\n') fileContentBuf in
    List.tl (List.rev liblist) *)

let rand_chr () = (Char.chr (97 + (Random.int 26)));;

let rec gen len str = 
    if len > 0 then 
        (let str = str ^ (String.make 1 (rand_chr ())) in
        gen (len - 1) str)
    else
        str
     
let rec generatekey len count = 
    if (count>0) then(
        (gen len "") :: generatekey len (count-1))
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
    (* Irmin_scylla.gc_meta_fun branch2; branch2 is a string and the branch which is sending its changes. make sure this is alwyas private. *)
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
    mergeOpr branchList public_branch_anchor repo  (*merge is returning unit*)

let buildLibrary ip client =
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
    
    create_or_get_private_branch repo client >>= fun private_branch_anchor ->
    
    (* let liblist = file_to_liblist (liblistpath ^ libindex) in  *)
    let liblist = generatekey 16 10 in  (*len count *)
    print_string (List.hd liblist);
    
    
    
    ignore @@ build liblist private_branch_anchor (client ^ "_private") repo client;

    ignore @@ publish_to_public repo client;

    refresh repo client >>= fun () ->

    (*ignore @@ squash repo (client ^ "_private") (client ^ "_public");*)

    Lwt.return_unit 


let _ =
        let hostip = Sys.argv.(1) in
        let client = Sys.argv.(2) in
        
        
        buildLibrary hostip client 

   