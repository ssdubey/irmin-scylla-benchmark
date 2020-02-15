open Lwt.Infix

module Scylla_kvStore = Irmin_scylla.KV(Irmin.Contents.String)

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

let getlib lib public_branch = 
    Scylla_kvStore.get public_branch [lib] >>= fun item ->
    print_string ("\njsut after insert " ^ item);
    Lwt.return_unit

let find_in_db lib public_branch = 
    try 
    print_string ("\nlib = " ^ lib);
    Scylla_kvStore.get public_branch [lib] >>= fun item ->
    print_string ("\n" ^ item);
    Lwt.return_true
    with 
    _ -> print_string "\nfailed find in db"; Lwt.return_false

let mergeBranches outBranch currentBranch = 
    Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) outBranch ~into:currentBranch

let rec mergeOpr branchList currentBranch repo =
    match branchList with 
    | h::t -> Scylla_kvStore.Branch.get repo h >>= fun commit ->
                Scylla_kvStore.of_commit commit >>= fun branch ->    
                ignore @@ mergeBranches branch currentBranch;
                mergeOpr t currentBranch repo 
    | _ -> Lwt.return_unit

let rec build liblist public_branch_anchor cbranch_string repo = (*cbranch as in current branch is only used for putting string in db*)
    (*merge branches in the db*) 
    Scylla_kvStore.Branch.list repo >>= fun branchList -> 
    ignore @@ mergeOpr branchList public_branch_anchor repo;

    match liblist with 
    | lib :: libls -> 
        find_in_db lib public_branch_anchor >>= fun boolval -> 
            (match boolval with 
            | false -> ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor [lib] ("downloaded at " ^ cbranch_string ^ "_" ^ lib);
                       (* ignore @@ getlib lib public_branch *)
            | true -> ());
        build libls public_branch_anchor cbranch_string repo
    | [] -> Lwt.return_unit

let rec printlist lst =
    match lst with 
    |h::t -> print_string h; printlist t
    |_ -> ()

let file_to_liblist liblistpath = 
    let fileContentBuf = readfile (open_in liblistpath) in 
    let liblist = String.split_on_char('\n') fileContentBuf in
    List.tl (List.rev liblist)

let create_or_get_branch repo ip = 
    try
    Scylla_kvStore.Branch.get repo (ip ^ "_public") >>= fun commit ->
    Scylla_kvStore.of_commit commit
    with _ -> 
    Scylla_kvStore.master repo >>= fun b_master ->
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_public")

let buildLibrary ip liblistpath =
    (* let ip = "172.17.0.2" in
    let liblistpath = "/home/shashank/work/benchmark_irminscylla/build_system/input/buildsystem/libreq" in *)
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
    create_or_get_branch repo ip >>= fun public_branch_anchor ->   
    let liblist = file_to_liblist liblistpath in
    ignore @@ build liblist public_branch_anchor (ip ^ "_public") repo;
        
Lwt.return_unit