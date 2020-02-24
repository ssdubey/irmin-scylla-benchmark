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
    let metadata = x.metadata @ y.metadata in
    Printf.printf "\nmetadata entries = %d \nxcount = %d, ycount = %d, oldcount= %d, final= %d" 
                                            (List.length metadata) x.count y.count old.count (x.count + y.count + old.count);
    let count = x.count + y.count + old.count in 
    Irmin.Merge.ok ({artifact = x.artifact; metadata = metadata; count = count})

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
        assert false (* This is never executed
                        (always raise Assert_failure). *)
    with
        End_of_file -> Buffer.contents buf

let getlib lib public_branch = 
    Scylla_kvStore.get public_branch [lib] >>= fun _ ->
    (* print_string ("\njsut after insert " ^ item); *)
    Lwt.return_unit

let find_in_db lib public_branch = 
    try 
    (* print_string ("\nlib = " ^ lib); *)
    Scylla_kvStore.get public_branch [lib] >>= fun _ ->
    (* print_string ("\n" ^ item); *)
    Lwt.return_true
    with 
    _ -> (*print_string "\nfailed find in db";*) Lwt.return_false

let mergeBranches outBranch currentBranch = 
    Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) outBranch ~into:currentBranch

let rec mergeOpr branchList currentBranch repo =
    match branchList with 
    | h::t -> Scylla_kvStore.Branch.get repo h >>= fun commit ->
                Scylla_kvStore.of_commit commit >>= fun branch ->    
                ignore @@ mergeBranches branch currentBranch;
                mergeOpr t currentBranch repo 
    | _ -> Lwt.return_unit

let createValue lib ip =
    let ts = string_of_float (Unix.gettimeofday ()) in 
    
    {artifact = lib; metadata = [(ip, ts)]; count = 1}

let updateValue item ip =
    let ts = string_of_float (Unix.gettimeofday ()) in
    let count = item.count + 1 in
    let metadata = (ip, ts) :: item.metadata in
    {artifact = item.artifact; metadata = metadata; count = count}

let rec build liblist public_branch_anchor cbranch_string repo ip = (*cbranch_string as in current branch is only used for putting string in db*)
    (*merge branches in the db*) 
    Scylla_kvStore.Branch.list repo >>= fun branchList -> 
    ignore @@ mergeOpr branchList public_branch_anchor repo;

    match liblist with 
    | lib :: libls -> 
        find_in_db lib public_branch_anchor >>= fun boolval -> 
            (match boolval with 
            | false -> (let v = createValue lib ip in
                        ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor [lib] v);
                       (* ignore @@ getlib lib public_branch *)
            | true -> 
                ignore (Scylla_kvStore.get public_branch_anchor [lib] >>= fun item ->
                        printdetails "old data" lib item;
                      let v = updateValue item ip in
                        Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor [lib] v)); 

        build libls public_branch_anchor cbranch_string repo ip;

    | [] -> Lwt.return_unit

(* let rec printlist lst =
    match lst with 
    |h::t -> (*print_string h;*) printlist t
    |_ -> () *)

let file_to_liblist liblistpath = 
    let fileContentBuf = readfile (open_in liblistpath) in 
    let liblist = String.split_on_char('\n') fileContentBuf in
    List.tl (List.rev liblist)

let create_or_get_branch repo ip = 
    try
    Scylla_kvStore.of_branch repo (ip ^ "_public")
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
    ignore @@ build liblist public_branch_anchor (ip ^ "_public") repo ip;

Lwt.return_unit 

(* let rec printmeta valuemeta =
    match valuemeta with 
    | h::t -> let ip, ts = h in print_string ("\n IP= " ^ ip); print_string ("  TS= " ^ ts); printmeta t
    | _ -> ()

let printdetails msg key value = 
    print_string ("\n msg : " ^ msg);
    print_string ("  key : " ^ key);
    print_string ("  value: artifact : " ^ value.artifact ^ "\n metadata: ");
    printmeta value.metadata;
    print_string ("  count = "); print_int value.count

let build_1 = {artifact = "artifact1"; metadata = [("IP1", "TS1")]; count = 1}
let build_2 = {artifact = "artifact2"; metadata = [("IP1", "TS2")]; count = 1}
let build_3 = {artifact = "artifact3"; metadata = [("IP1", "TS3")]; count = 1}
let build_4 = {artifact = "artifact1"; metadata = [("IP2", "TS4")]; count = 1}
let build_5 = {artifact = "artifact2"; metadata = [("IP2", "TS5")]; count = 1}
let build_6 = {artifact = "artifact3"; metadata = [("IP2", "TS6")]; count = 1}

let testcase1 () = 
    let ip = "127.0.0.1" in
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
    Scylla_kvStore.master repo >>= fun b_master ->  
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_public") >>= fun public_branch_anchor ->  


    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                b_master ["key1"] build_1;

    Scylla_kvStore.get b_master ["key1"] >>= fun item ->
    printdetails "after inserting key1 at client1" "key1" item;

    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                b_master ["key2"] build_2;

    Scylla_kvStore.get b_master ["key2"] >>= fun item ->
    printdetails "after inserting key2 at client1" "key2" item;

    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                b_master ["key3"] build_3;

    Scylla_kvStore.get b_master ["key3"] >>= fun item ->
    printdetails "after inserting key3 at client1" "key3" item;


    

    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor ["key1"] build_4;

    Scylla_kvStore.get public_branch_anchor ["key1"] >>= fun item ->
    printdetails "after inserting key1 at client2" "key1" item;

    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor ["key2"] build_5;

    Scylla_kvStore.get public_branch_anchor ["key2"] >>= fun item ->
    printdetails "after inserting key2 at client2" "key2" item;
    
    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor ["key3"] build_6;

    Scylla_kvStore.get public_branch_anchor ["key3"] >>= fun item ->
    printdetails "after inserting key3 at client2" "key3" item;
    


    ignore @@ Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) public_branch_anchor ~into:b_master; (*~into is always the second argument in the merge function*)

    Scylla_kvStore.get b_master ["key1"] >>= fun item ->
    printdetails "after merging two branches key1 at client1" "key1" item;
    
    Lwt.return_unit

let testcase2 () = 
    let ip = "127.0.0.1" in
    let conf = Irmin_scylla.config ip in
    Scylla_kvStore.Repo.v conf >>= fun repo ->
    Scylla_kvStore.master repo >>= fun b_master ->  
    Scylla_kvStore.clone ~src:b_master ~dst:(ip ^ "_public") >>= fun public_branch_anchor ->  

    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                b_master ["key1"] build_1;

    Scylla_kvStore.get b_master ["key1"] >>= fun item ->
    printdetails "after inserting key1 at client1" "key1" item;


    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                public_branch_anchor ["key1"] build_4;

    Scylla_kvStore.get public_branch_anchor ["key1"] >>= fun item ->
    printdetails "after inserting key1 at client2" "key1" item;


    ignore @@ Scylla_kvStore.set_exn ~info:(fun () -> Irmin.Info.empty) 
                                                b_master ["key1"] build_1;

    Scylla_kvStore.get b_master ["key1"] >>= fun item ->
    printdetails "after inserting key1 at client1" "key1" item;


    ignore @@ Scylla_kvStore.merge_into ~info:(fun () -> Irmin.Info.empty) public_branch_anchor ~into:b_master;
    
    Scylla_kvStore.get b_master ["key1"] >>= fun item ->
    printdetails "after merging two branches key1 at client1" "key1" item;

    Lwt.return_unit

let main () =
    (* testcase1 (); *)
    testcase2 () *)
    