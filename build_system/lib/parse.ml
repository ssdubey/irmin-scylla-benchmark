let rec printList kvpairlist = 
match kvpairlist with
|h::t -> let k, v = h in
        print_string ("\nkey =" ^ k); print_string ("  value =" ^ v) ;
        printList t
| _ -> ()


(*split on space and store in list*)
let rec kvPairList_Helper kvlist contentPerLine =
let kvpairlist = (match contentPerLine with
| p::p_lst -> (let pair = String.split_on_char(' ') p in
              let a = (match pair with  
                      | k::v::[] -> (k,v) 
                      | _ -> ("","")) in
                  kvPairList_Helper (a::kvlist) p_lst)
| _ -> kvlist) in         (* print_string ("\nkvpairlist length in rec func: " ); print_int (List.length kvpairlist); *)
(* printList kvpairlist; *)
kvpairlist


(*first split on \n then split on , and store in list*)
let contentToKVpairList fileContentBuf =
  let contentPerLine = String.split_on_char('\n') fileContentBuf in
  (* print_string ("\nno. of entries: " ); print_int (List.length contentPerLine); *)
  let kvpairlist = kvPairList_Helper [] contentPerLine in			
  List.tl kvpairlist  (**first element of the list is empty *)

(* let rec printList kvpairlist = 
match kvpairlist with
|h::t -> let k, v = h in
        print_string ("\nkey =" ^ k); print_string ("  value =" ^ v) ;
        printList t
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

(** starting function *)
(*path is the path to file which contains input data as key and value separated by rows (see below)*)
let file_to_kvpair path = 
  let fileContentBuf = readfile (open_in path) in 
  let kvpairlist = contentToKVpairList fileContentBuf in
(* print_string ("\nkvpairlist length in main:" );print_int (List.length kvpairlist); *)
(* printList kvpairlist; *)
kvpairlist

(* let formatkey k = (* 172.17.0.2,Irmin *)
  let klist = String.split_on_char(',') k in 
  klist *)

(**returning string for prototype. it should return the binary. Actually I want it to return the zip of binaries for a given lib *)
let createArtifact library = 
  let cmd = ("opam install " ^ library) in  
  cmd

(** this will return the (lib name, zip of binaries) in real design *)
let rec processlib liblist = 
  match liblist with 
  | h :: [] -> [(h, createArtifact h)]
  | h :: t -> let something = processlib t in 
              (h, createArtifact h) :: something
  | _ -> [("invalid lib", "no artifact")]