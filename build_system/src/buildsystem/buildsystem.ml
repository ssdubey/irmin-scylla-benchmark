open Lwt.Infix

type distbuild = {artifact: string ; mutable metadata: (string * string) list ; count : int } 

module Distbuild = struct
  
  type t = distbuild
  
  let t = 
        let open Irmin.Type in 
        record "distbuild" (fun artifact metadata count ->
            {artifact; metadata; count; })
        |+ field "artifact" string (fun t -> t.artifact)
        |+ field "metadata" (list (pair string string)) (fun t -> t.metadata)
        |+ field "count" int (fun t -> t.count)
        |> sealr

  let merge_build ~old x y =
    
    print_string "\nin merge_build";
    
    let open Irmin.Merge.Infix in
    old () >>=* fun old ->
    let old = match old with None -> {artifact = "dummy"; metadata = [("IP", "TS")]; count = 0} | Some o -> o in 
    
    (*not considering the point where artifacts against the same key would be different due to version change, for eg.*)
    let metadata = x.metadata @ y.metadata in (*delete the duplicates from the list*)
    Printf.printf "\nmetadata entries = %d \nxcount = %d, ycount = %d, oldcount= %d, final= %d" 
                                            (List.length metadata) x.count y.count old.count (x.count + y.count - old.count);
    let count = x.count + y.count - old.count in 
    Irmin.Merge.ok ({artifact = x.artifact; metadata = metadata; count = count})

    (* Irmin.Merge.ok ({artifact = "dummy"; metadata = [("IP", "TS")]; count = 0}) *)

  (* let merge = Irmin.Merge.(option (default t)) *)
  let merge = Irmin.Merge.(option (v t merge_build))

end


module Scylla_kvStore = Irmin_scylla.KV(Distbuild)

let rec printmeta valuemeta =
    match valuemeta with 
    | h::t -> let ip, ts = h in print_string ("\n IP= " ^ ip); print_string ("  TS= " ^ ts); printmeta t
    | _ -> ()

let printdetails msg key value = 
    print_string ("\n msg : " ^ msg);
    print_string ("  key : " ^ key);
    print_string ("  value: artifact : " ^ value.artifact ^ "\n metadata: ");
    printmeta value.metadata;
    print_string ("  count = "); print_int value.count; print_string "\n\n"

    
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

(* let getlib lib public_branch = 
    Scylla_kvStore.get public_branch [lib] >>= fun _ ->
    Lwt.return_unit *)

let find_in_db lib private_branch = 
    try 
    Scylla_kvStore.get private_branch [lib] >>= fun _ ->
    Lwt.return_true
    with 
    _ -> Lwt.return_false

let mergeBranches outBranch currentBranch = 
    Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) outBranch ~into:currentBranch

let rec mergeOpr branchList currentBranch repo =
    match branchList with 
    | h::t -> print_string ("\ncurrent branch to merge: " ^ h);Scylla_kvStore.of_branch repo h >>= fun branch ->    
                ignore @@ mergeBranches branch currentBranch;
                mergeOpr t currentBranch repo 
    | _ -> print_string "branch list empty"; Lwt.return_unit

let refresh repo public_branch_anchor =
    (*merge current branch with the detached head of other*) 
    Scylla_kvStore.Branch.list repo >>= fun branchList -> 
    mergeOpr branchList public_branch_anchor repo  (*merge is returning unit*)

let createValue lib ip =
    let ts = string_of_float (Unix.gettimeofday ()) in 
    
    {artifact = lib; metadata = [(ip, ts)]; count = 1}

let updateValue item ip =
    let ts = string_of_float (Unix.gettimeofday ()) in
    let count = item.count + 1 in
    let metadata = (ip, ts) :: item.metadata in
    {artifact = item.artifact; metadata = metadata; count = count}

let rec build liblist private_branch_anchor cbranch_string repo ip = (*cbranch_string as in current branch is only used for putting string in db*)
    match liblist with 
    | lib :: libls -> 
        find_in_db lib private_branch_anchor >>= fun boolval -> 
            (match boolval with 
            | false -> (let v = createValue lib ip in
                        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                private_branch_anchor [lib] v);
            | true -> 
                ignore (Scylla_kvStore.get private_branch_anchor [lib] >>= fun item ->
                        printdetails "old data" lib item;
                      let v = updateValue item ip in
                        Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                private_branch_anchor [lib] v));
            
            ignore (Scylla_kvStore.get private_branch_anchor [lib] >>= fun item ->
                        printdetails "new data" lib item;
                        Lwt.return_unit);                                     

        build libls private_branch_anchor cbranch_string repo ip;

    | [] -> Lwt.return_unit

let file_to_liblist liblistpath = 
    let fileContentBuf = readfile (open_in liblistpath) in 
    let liblist = String.split_on_char('\n') fileContentBuf in
    List.tl (List.rev liblist)

let create_or_get_public_branch repo ip = 
    try
    Scylla_kvStore.of_branch repo (ip ^ "_public")
    with _ -> 
    Scylla_kvStore.master repo >>= fun b_master ->
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_public")

let create_or_get_private_branch repo ip = 
    try
    Scylla_kvStore.of_branch repo (ip ^ "_private")
    with _ -> 
    Scylla_kvStore.master repo >>= fun b_master ->
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_private")

let testfun public_branch_anchor lib msg =
    Scylla_kvStore.get public_branch_anchor [lib] >>= fun item ->
                        printdetails msg lib item;
    Lwt.return_unit

let publish branch1 branch2 = (*changes of branch2 will merge into branch1*)
    (* Irmin_scylla.gc_meta_fun branch2; branch2 is a string and the branch which is sending its changes. make sure this is alwyas private. *)
    Scylla_kvStore.merge_with_branch ~info:(fun () -> Irmin.Info.empty) branch1 branch2

(*publishing the changes*)
let publish_to_public repo ip =
    create_or_get_public_branch repo ip >>= fun public_branch_anchor ->
    (*changes of 2nd arg branch will merge into first*)
    ignore @@ publish public_branch_anchor (ip ^ "_private");
    Lwt.return_unit 

(* let rec resolve_lwt lst = *)
    

let squash repo private_branch_str public_branch_str = 
    Scylla_kvStore.Branch.get repo private_branch_str >>= fun latest_cmt ->
    let latest_tree = Scylla_kvStore.Commit.tree latest_cmt in

    Scylla_kvStore.Branch.get repo public_branch_str >>= fun old_commit ->
    Scylla_kvStore.Commit.v repo ~info:(Irmin.Info.empty) ~parents:[Scylla_kvStore.Commit.hash old_commit] latest_tree >>= fun new_commit -> 
    (* print "%s" (Irmin.Type.to_string (Scylla_kvStore.Commit.t repo) new_commit); *)

    Scylla_kvStore.of_branch repo private_branch_str >>= fun private_branch_anchor ->
    Scylla_kvStore.Head.set private_branch_anchor new_commit

    (* Lwt.return_unit *)

let buildLibrary ip liblistpath =
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
     ignore liblistpath;
    (* create_or_get_private_branch repo ip >>= fun private_branch_anchor ->  *)
    
    (* let liblist = file_to_liblist liblistpath in *)
    (* refresh repo public_branch_anchor >>= fun () -> *)
    (* ignore @@ build liblist private_branch_anchor (ip ^ "_private") repo ip; *)

    (* ignore @@ publish_to_public repo ip; *)

    ignore @@ squash repo (ip ^ "_private") (ip ^ "_public");

    Lwt.return_unit 

    (*In this code private branch takes up all the updates and then push everything to the public brnach. All the functioning at public branch
    like refresh is commented. *)


(* Squash extra: *)
    (* let parent_hash_list = Scylla_kvStore.Commit.parents old_commit in  *)
    (* let parent_commit_option_list = List.map (fun x -> Lwt_main.run (Scylla_kvStore.Commit.of_hash repo x)) parent_hash_list in
    let parent_commit_list = List.map (fun x -> match x with | Some y -> y | None -> failwith "bad commit") parent_commit_option_list in 
    let hash_parent_list = List.map (fun x -> Scylla_kvStore.Commit.hash x) parent_commit_list in  *)

    (* Scylla_kvStore.Commit.v repo ~info:(Irmin.Info.empty) ~parents:parent_hash_list latest_tree >>= fun new_commit ->  *)
    